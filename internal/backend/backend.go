// Package backend simulates the resource backend (compute/storage) that the
// orchestrator drives. It is fully local, needs no credentials and models
// flaky behavior deterministically for tests. Each Ensure* operation is
// idempotent keyed on the caller-provided resource id: calling it repeatedly
// (including after an injected failure) never creates duplicate resources and
// never reports a state-changing side effect twice. The Counts() accessor lets
// tests assert there were no duplicate resources or leaked side effects.
package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// Status of a simulated backing resource.
type Status string

const (
	StatusAbsent  Status = "absent"
	StatusStopped Status = "stopped"
	StatusRunning Status = "running"
)

// ResourceState is the observed state of a backing resource.
type ResourceState struct {
	ID      string
	Res     domain.Resources
	Status  Status
	Exists  bool
	Running bool
}

// Fault is a deterministic fault plan keyed by operation and attempt number.
// Attempts are 1-based per (resource id, operation). The attempt set lists the
// attempts that must fail with a transient backend error; later attempts
// succeed, modeling a temporary wobble that convergence must survive.
type Fault struct {
	// FailCreateAttempts / FailStartAttempts / FailStopAttempts /
	// FailDeleteAttempts: 1-based attempts to fail, e.g. {1,2}.
	FailCreateAttempts []int
	FailStartAttempts  []int
	FailStopAttempts   []int
	FailDeleteAttempts []int
	// FailInspectAttempts: attempts at which Inspect fails transiently.
	FailInspectAttempts []int
	// PermanentX: when true the operation keeps failing with a *terminal*
	// backend error; convergence must give up, mark the environment failed and
	// release its reservation rather than retry forever.
	PermanentCreate bool
	PermanentStart  bool
	PermanentStop   bool
	PermanentDelete bool
}

// IsTransient reports whether a backend error is retryable. Terminal/permanent
// backend errors return false.
func IsTransient(err error) bool {
	e := domain.AsError(err)
	return e != nil && e.Kind == domain.KindBackend && e.Distinct == "transient"
}

// Backend is the resource-plane abstraction.
type Backend interface {
	EnsureCreated(ctx context.Context, id string, res domain.Resources) error
	EnsureStarted(ctx context.Context, id string) error
	EnsureStopped(ctx context.Context, id string) error
	EnsureDeleted(ctx context.Context, id string) error
	Inspect(ctx context.Context, id string) (ResourceState, error)
}

// record is one simulated resource.
type record struct {
	id     string
	res    domain.Resources
	status Status
}

// FakeBackend is an in-memory, concurrency-safe Backend with deterministic
// fault injection. One orchestrator instance maps one environment to one
// resource id; that keeps backing-state reasoning explicit and leak-checking
// trivial (surviving records == leaked resources).
type FakeBackend struct {
	mu      sync.Mutex
	records map[string]*record
	// attempts counts per "id|op".
	attempts map[string]int
	fault    Fault

	// side-effect counters: incremented only when an operation actually
	// changes state, so tests can assert idempotency ("no extra side effects").
	createdN int
	startedN int
	stoppedN int
	deletedN int
}

// NewFakeBackend constructs an empty fake backend.
func NewFakeBackend() *FakeBackend {
	return &FakeBackend{
		records:  map[string]*record{},
		attempts: map[string]int{},
	}
}

// WithFault configures the deterministic fault plan (returns the receiver).
func (b *FakeBackend) WithFault(f Fault) *FakeBackend {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fault = f
	b.attempts = map[string]int{}
	return b
}

// Counts returns the number of real state-changing side effects performed.
type Counts struct {
	Created, Started, Stopped, Deleted int
}

// Counts returns side-effect counters.
func (b *FakeBackend) Counts() Counts {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Counts{b.createdN, b.startedN, b.stoppedN, b.deletedN}
}

// Existing returns the ids of currently present resources (leak detector).
func (b *FakeBackend) Existing() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]string, 0, len(b.records))
	for id := range b.records {
		ids = append(ids, id)
	}
	return ids
}

// ForceDeleteForTest removes a resource regardless of its state, simulating an
// out-of-band loss of the backing resource (test only).
func (b *FakeBackend) ForceDeleteForTest(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.records, id)
}

func contains(xs []int, x int) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// transientErr models a retryable resource-plane failure.
func transientErr(op, id string, attempt int) error {
	return domain.NewError(domain.KindBackend, "transient",
		"simulated transient %s failure for %s on attempt %d", op, id, attempt)
}

// permanentErr models a non-retryable resource-plane failure.
func permanentErr(op, id string) error {
	return domain.NewError(domain.KindBackend, "permanent",
		"simulated permanent %s failure for %s", op, id)
}

func (b *FakeBackend) tick(id, op string, failAttempts []int, permanent bool) (int, bool, bool) {
	key := id + "|" + op
	b.attempts[key]++
	n := b.attempts[key]
	return n, contains(failAttempts, n), permanent
}

// EnsureCreated makes sure a stopped resource with id exists. It is a no-op
// (and counted as no side effect) if the resource already exists.
func (b *FakeBackend) EnsureCreated(ctx context.Context, id string, res domain.Resources) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n, fail, perm := b.tick(id, "create", b.fault.FailCreateAttempts, b.fault.PermanentCreate)
	if fail {
		return transientErr("create", id, n)
	}
	if perm {
		return permanentErr("create", id)
	}
	if r, ok := b.records[id]; ok {
		// Idempotent reconciliation: keep the requested footprint aligned.
		r.res = res
		return nil
	}
	if res.CPU < 0 || res.Memory < 0 || res.Storage < 0 {
		return domain.NewError(domain.KindValidation, "negative_resources",
			"refusing negative resources for %s", id)
	}
	b.records[id] = &record{id: id, res: res, status: StatusStopped}
	b.createdN++
	return nil
}

// EnsureStarted makes sure an existing resource is running.
func (b *FakeBackend) EnsureStarted(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n, fail, perm := b.tick(id, "start", b.fault.FailStartAttempts, b.fault.PermanentStart)
	if fail {
		return transientErr("start", id, n)
	}
	if perm {
		return permanentErr("start", id)
	}
	r, ok := b.records[id]
	if !ok {
		return domain.NewError(domain.KindBackend, "missing_resource",
			"cannot start %s: resource does not exist", id)
	}
	if r.status == StatusRunning {
		return nil
	}
	r.status = StatusRunning
	b.startedN++
	return nil
}

// EnsureStopped makes sure an existing resource is stopped (but still present).
func (b *FakeBackend) EnsureStopped(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n, fail, perm := b.tick(id, "stop", b.fault.FailStopAttempts, b.fault.PermanentStop)
	if fail {
		return transientErr("stop", id, n)
	}
	if perm {
		return permanentErr("stop", id)
	}
	r, ok := b.records[id]
	if !ok {
		// Already gone == target state satisfied; enables safe teardown retry.
		return nil
	}
	if r.status == StatusStopped {
		return nil
	}
	r.status = StatusStopped
	b.stoppedN++
	return nil
}

// EnsureDeleted makes sure the resource no longer exists.
func (b *FakeBackend) EnsureDeleted(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n, fail, perm := b.tick(id, "delete", b.fault.FailDeleteAttempts, b.fault.PermanentDelete)
	if fail {
		return transientErr("delete", id, n)
	}
	if perm {
		return permanentErr("delete", id)
	}
	r, ok := b.records[id]
	if !ok {
		return nil
	}
	if r.status != StatusStopped {
		// A running resource must be stopped first so teardown is orderly;
		// this also makes a partial crash (stopped but not deleted) resumable.
		return domain.NewError(domain.KindBackend, "not_stopped",
			"cannot delete running resource %s", id)
	}
	delete(b.records, id)
	b.deletedN++
	return nil
}

// Inspect returns the observed state of a resource. Missing is not an error:
// Exists=false is how reconciliation distinguishes a partial create.
func (b *FakeBackend) Inspect(ctx context.Context, id string) (ResourceState, error) {
	if err := ctx.Err(); err != nil {
		return ResourceState{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n, fail, _ := b.tick(id, "inspect", b.fault.FailInspectAttempts, false)
	if fail {
		return ResourceState{}, transientErr("inspect", id, n)
	}
	r, ok := b.records[id]
	if !ok {
		return ResourceState{ID: id, Status: StatusAbsent}, nil
	}
	return ResourceState{
		ID:      id,
		Res:     r.res,
		Status:  r.status,
		Exists:  true,
		Running: r.status == StatusRunning,
	}, nil
}

// String aids debugging/logging.
func (s ResourceState) String() string {
	return fmt.Sprintf("Resource(%s,status=%s,exists=%t,running=%t)", s.ID, s.Status, s.Exists, s.Running)
}
