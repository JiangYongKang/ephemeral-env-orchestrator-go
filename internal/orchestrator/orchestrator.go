// Package orchestrator implements declarative lifecycle management for local
// ephemeral development environments:
//
//   - CRUD with ownership, quota, TTL and immutable-field validation.
//   - A persisted, deterministic-priority operation queue per environment.
//   - A resumable reconciliation loop converging desired vs. observed state,
//     safe across concurrent triggers, process restarts and partial failures.
//   - Multi-tenant isolation and atomic quota settlement derived from live
//     allocations (no counters that can drift on crash).
//
// All state lives in one atomically-written local JSON document; no real
// cluster, external service or credentials are required.
package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/quota"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/resource"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/store"
)

// Role of an actor.
type Role string

const (
	RoleOwner Role = "owner"
	RoleAdmin Role = "admin"
)

// Actor identifies the caller for authorization decisions.
type Actor struct {
	Name     string
	TenantID string
	Role     Role
}

// CreateInput is a declarative create request.
type CreateInput struct {
	Name       string
	Image      string
	Replicas   int
	Resources  model.Resources
	TTLSeconds int
}

// UpdateInput carries the mutable subset of a declaration. Owner is accepted
// only to prove the immutable-field check rejects it.
type UpdateInput struct {
	Owner      *string
	Image      *string
	Replicas   *int
	Resources  *model.Resources
	TTLSeconds *int
}

// MaxBackendAttempts bounds transient-failure retries before an environment
// is explained as Failed.
const MaxBackendAttempts = 5

// tickInterval is the reconciliation period (overridable via WithTickInterval).
var tickInterval = 20 * time.Millisecond

// opPriority defines deterministic conflict resolution within one
// environment: lower value runs first. Delete wins (safety/release), then
// stop, update, start, create.
var opPriority = map[model.OpKind]int{
	model.OpDelete: 0,
	model.OpStop:   1,
	model.OpUpdate: 2,
	model.OpStart:  3,
	model.OpCreate: 4,
}

// Orchestrator is the local, single-process control plane.
type Orchestrator struct {
	mu      sync.Mutex
	st      *model.State
	store   *store.Store
	quota   *quota.Checker
	backend *resource.Backend
	log     *slog.Logger
	nowFn   func() time.Time

	stopCh chan struct{}
	doneCh chan struct{}
}

// Option customizes an Orchestrator (used by tests and the demo).
type Option func(*Orchestrator)

// WithLogger sets the structured logger.
func WithLogger(l *slog.Logger) Option {
	return func(o *Orchestrator) { o.log = l }
}

// WithClock injects a clock (used for deterministic TTL tests).
func WithClock(now func() time.Time) Option {
	return func(o *Orchestrator) { o.nowFn = now }
}

// WithTickInterval overrides the reconciliation period.
func WithTickInterval(d time.Duration) Option {
	return func(o *Orchestrator) { tickInterval = d }
}

// New loads persisted state, reattaches already-materialized simulated
// resources (control-plane restart), and starts the reconciliation loop.
func New(st *store.Store, q *quota.Checker, b *resource.Backend, opts ...Option) (*Orchestrator, error) {
	loaded, err := st.Load()
	if err != nil {
		return nil, fmt.Errorf("orchestrator: load state: %w", err)
	}
	o := &Orchestrator{
		st:      loaded,
		store:   st,
		quota:   q,
		backend: b,
		log:     slog.Default(),
		nowFn:   time.Now,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt(o)
	}

	// Restart recovery: re-register every durable allocation with the
	// simulated infrastructure, then reconcile drift on the next tick.
	var allocs []model.Allocation
	for _, e := range o.st.Envs {
		allocs = append(allocs, e.Allocations...)
	}
	b.Reattach(allocs)
	o.recoverLocked()
	if err := o.store.Save(o.st); err != nil {
		return nil, fmt.Errorf("orchestrator: persist recovered state: %w", err)
	}
	o.log.Info("orchestrator started",
		"environments", len(o.st.Envs), "pendingOps", o.countPendingLocked())

	go o.runLoop()
	return o, nil
}

// now returns the current (possibly injected) time.
func (o *Orchestrator) now() time.Time { return o.nowFn() }

// saveLocked persists while holding the state lock.
func (o *Orchestrator) saveLocked() error {
	o.st.UpdatedAt = o.now()
	return o.store.Save(o.st)
}

// nextSeqLocked returns a monotonic, restart-stable sequence number.
func (o *Orchestrator) nextSeqLocked() int64 {
	o.st.Seq++
	return o.st.Seq
}

// findByNameLocked locates an active (non-tombstone) env by tenant+name.
func (o *Orchestrator) findByNameLocked(tenantID, name string) *model.Environment {
	for _, e := range o.st.Envs {
		if !e.Deleted && e.Spec.TenantID == tenantID && e.Spec.Name == name {
			return e
		}
	}
	return nil
}

// envLocked loads an env by ID, treating finalized tombstones as absent.
func (o *Orchestrator) envLocked(id string) *model.Environment {
	e := o.st.Envs[id]
	if e == nil || e.Deleted {
		return nil
	}
	return e
}

// cloneEnv returns a deep copy so callers can read an environment outside
// the orchestrator lock without racing the reconciliation loop.
func cloneEnv(e *model.Environment) *model.Environment {
	if e == nil {
		return nil
	}
	cp := *e
	if e.Allocations != nil {
		cp.Allocations = append([]model.Allocation(nil), e.Allocations...)
	}
	return &cp
}

// authorize enforces tenant isolation and ownership.
//   - missing actor identity -> Unauthorized
//   - cross-tenant access      -> Forbidden
//   - non-owner owner-role     -> Forbidden
//   - admin                    -> allowed
func (o *Orchestrator) authorize(actor Actor, e *model.Environment) error {
	if actor.Name == "" || actor.TenantID == "" {
		return errors.New(errors.KindUnauthorized, "unauthenticated actor: name and tenant required")
	}
	switch actor.Role {
	case RoleAdmin:
		return nil
	case RoleOwner:
		if actor.TenantID != e.Spec.TenantID {
			return errors.New(errors.KindForbidden,
				"cross-tenant access denied: actor tenant "+actor.TenantID+" != env tenant "+e.Spec.TenantID)
		}
		if actor.Name != e.Spec.Owner {
			return errors.New(errors.KindForbidden,
				"access denied: actor "+actor.Name+" is not owner "+e.Spec.Owner)
		}
		return nil
	default:
		return errors.New(errors.KindUnauthorized, "unknown actor role: "+string(actor.Role))
	}
}

// requireAdmin rejects non-admin actors with Forbidden.
func requireAdmin(actor Actor, action string) error {
	if actor.Name == "" || actor.TenantID == "" {
		return errors.New(errors.KindUnauthorized, "unauthenticated actor")
	}
	if actor.Role != RoleAdmin {
		return errors.New(errors.KindForbidden, action+" requires admin role")
	}
	return nil
}

// validateResources rejects non-positive or negative resource vectors.
func validateResources(r model.Resources) error {
	switch {
	case r.CPUMilli <= 0:
		return errors.New(errors.KindValidation, "resources.cpuMilli must be > 0")
	case r.MemoryMB <= 0:
		return errors.New(errors.KindValidation, "resources.memoryMB must be > 0")
	case r.StorageGB <= 0:
		return errors.New(errors.KindValidation, "resources.storageGB must be > 0")
	}
	return nil
}

// validateSpec validates the create-time declaration.
func validateSpec(name, image string, replicas int, res model.Resources, ttl int) error {
	if name == "" {
		return errors.New(errors.KindValidation, "environment name is required")
	}
	if image == "" {
		return errors.New(errors.KindValidation, "image is required")
	}
	if replicas < 1 || replicas > 100 {
		return errors.New(errors.KindValidation, "replicas must be within [1,100]")
	}
	if err := validateResources(res); err != nil {
		return err
	}
	if ttl < 0 {
		return errors.New(errors.KindValidation, "ttlSeconds must be >= 0 (0 means no expiry)")
	}
	return nil
}

// enqueueLocked appends an operation, suppressing duplicate pending ops so
// repeated submissions create no extra work or side effects.
func (o *Orchestrator) enqueueLocked(kind model.OpKind, envID string, actor Actor, newSpec *model.EnvSpec) *model.Operation {
	for _, op := range o.st.Operations {
		if op.EnvID == envID && !op.Done && op.Kind == kind {
			if kind != model.OpUpdate || equalSpec(op.NewSpec, newSpec) {
				return op // identical intent already queued: idempotent
			}
		}
	}
	op := &model.Operation{
		ID:        "op-" + strconv.FormatInt(o.nextSeqLocked(), 10),
		EnvID:     envID,
		TenantID:  actor.TenantID,
		Actor:     actor.Name,
		Kind:      kind,
		Priority:  opPriority[kind],
		Seq:       o.st.Seq,
		NewSpec:   newSpec,
		CreatedAt: o.now(),
	}
	o.st.Operations = append(o.st.Operations, op)
	return op
}

func equalSpec(a, b *model.EnvSpec) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// pendingOpForLocked reports whether env has any unfinished operation.
func (o *Orchestrator) pendingOpForLocked(envID string) bool {
	for _, op := range o.st.Operations {
		if op.EnvID == envID && !op.Done {
			return true
		}
	}
	return false
}

func (o *Orchestrator) countPendingLocked() int {
	n := 0
	for _, op := range o.st.Operations {
		if !op.Done {
			n++
		}
	}
	return n
}

// CreateEnvironment validates, authorizes and records a create declaration
// plus a queued Create op. Replays (same tenant+name) return the existing
// environment with a classified Conflict error and no side effects.
func (o *Orchestrator) CreateEnvironment(ctx context.Context, actor Actor, in CreateInput) (*model.Environment, error) {
	if err := validateSpec(in.Name, in.Image, in.Replicas, in.Resources, in.TTLSeconds); err != nil {
		o.log.Warn("create rejected: invalid spec", "actor", actor.Name, "name", in.Name, "reason", err)
		return nil, err
	}
	if actor.Name == "" || actor.TenantID == "" {
		return nil, errors.New(errors.KindUnauthorized, "unauthenticated actor")
	}
	if actor.Role != RoleOwner && actor.Role != RoleAdmin {
		return nil, errors.New(errors.KindUnauthorized, "unknown actor role")
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if existing := o.findByNameLocked(actor.TenantID, in.Name); existing != nil {
		o.log.Info("create replay: returning existing environment (no new side effect)",
			"envId", existing.ID, "tenant", actor.TenantID, "name", in.Name)
		return cloneEnv(existing), errors.New(errors.KindConflict,
			"environment "+in.Name+" already exists in tenant "+actor.TenantID+" (id="+existing.ID+")")
	}

	// Atomic admission check against tenant quota and cluster capacity.
	requested := in.Resources.Mul(in.Replicas)
	if ok, reason := o.quota.Fits(o.st, actor.TenantID, requested); !ok {
		o.log.Warn("create rejected: quota/capacity", "actor", actor.Name, "requested", requested, "reason", reason)
		return nil, errors.New(errors.KindQuota, reason)
	}

	now := o.now()
	envID := "env-" + strconv.FormatInt(o.nextSeqLocked(), 10)
	spec := model.EnvSpec{
		Name:       in.Name,
		Owner:      actor.Name,
		TenantID:   actor.TenantID,
		Image:      in.Image,
		Replicas:   in.Replicas,
		Resources:  in.Resources,
		TTLSeconds: in.TTLSeconds,
		CreatedAt:  now,
	}
	expiry := time.Time{}
	if in.TTLSeconds > 0 {
		expiry = now.Add(time.Duration(in.TTLSeconds) * time.Second)
	}
	env := &model.Environment{
		ID:         envID,
		Spec:       spec,
		Desired:    model.DesiredRunning,
		Phase:      model.PhasePending,
		Generation: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  expiry,
	}
	o.st.Envs[envID] = env
	op := o.enqueueLocked(model.OpCreate, envID, actor, nil)
	if err := o.saveLocked(); err != nil {
		return nil, errors.New(errors.KindInternal, err.Error())
	}
	created := cloneEnv(env)
	o.log.Info("environment declared",
		"envId", envID, "tenant", actor.TenantID, "owner", actor.Name,
		"name", in.Name, "replicas", in.Replicas, "requested", requested,
		"expiresAt", expiry, "op", op.ID)
	return created, nil
}

// UpdateEnvironment validates immutable fields, authorizes and queues an
// update carrying the fully-merged candidate spec.
func (o *Orchestrator) UpdateEnvironment(ctx context.Context, actor Actor, envID string, in UpdateInput) (*model.Environment, error) {
	if in.Owner != nil {
		return nil, errors.New(errors.KindImmutable, "spec.owner is immutable after creation")
	}
	if in.Replicas != nil && (*in.Replicas < 1 || *in.Replicas > 100) {
		return nil, errors.New(errors.KindValidation, "replicas must be within [1,100]")
	}
	if in.Resources != nil {
		if err := validateResources(*in.Resources); err != nil {
			return nil, err
		}
	}
	if in.TTLSeconds != nil && *in.TTLSeconds < 0 {
		return nil, errors.New(errors.KindValidation, "ttlSeconds must be >= 0")
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	env := o.envLocked(envID)
	if env == nil {
		return nil, errors.New(errors.KindNotFound, "environment not found: "+envID)
	}
	if err := o.authorize(actor, env); err != nil {
		o.log.Warn("update denied", "envId", envID, "actor", actor.Name, "reason", err)
		return nil, err
	}

	candidate := env.Spec
	renewed := false
	if in.Image != nil {
		if *in.Image == "" {
			return nil, errors.New(errors.KindValidation, "image is required")
		}
		candidate.Image = *in.Image
	}
	if in.Replicas != nil {
		candidate.Replicas = *in.Replicas
	}
	if in.Resources != nil {
		candidate.Resources = *in.Resources
	}
	if in.TTLSeconds != nil {
		candidate.TTLSeconds = *in.TTLSeconds
		// Extending/resetting TTL moves expiry and renews an expired env.
		if *in.TTLSeconds > 0 {
			env.ExpiresAt = o.now().Add(time.Duration(*in.TTLSeconds) * time.Second)
		} else {
			env.ExpiresAt = time.Time{}
		}
		if env.Phase == model.PhaseExpired {
			renewed = true
			env.Phase = model.PhasePending
		}
	}

	oldReserved := model.Resources{}
	if env.Desired == model.DesiredRunning && !renewed {
		oldReserved = env.Spec.Requested()
	}
	// A TTL renewal flips the env back to Running and re-acquires reservation.
	targetDesiredRunning := env.Desired == model.DesiredRunning || renewed
	newReserved := model.Resources{}
	if targetDesiredRunning {
		newReserved = candidate.Requested()
	}
	if ok, reason := o.quota.CheckChange(o.st, actor.TenantID, oldReserved, newReserved); !ok {
		o.log.Warn("update rejected: quota", "envId", envID, "old", oldReserved, "new", newReserved, "reason", reason)
		return nil, errors.New(errors.KindQuota, reason)
	}
	if renewed {
		env.Desired = model.DesiredRunning
	}

	specCopy := candidate
	op := o.enqueueLocked(model.OpUpdate, envID, actor, &specCopy)
	env.UpdatedAt = o.now()
	if err := o.saveLocked(); err != nil {
		return nil, errors.New(errors.KindInternal, err.Error())
	}
	o.log.Info("update queued", "envId", envID, "actor", actor.Name, "op", op.ID,
		"image", candidate.Image, "replicas", candidate.Replicas, "ttl", candidate.TTLSeconds)
	return cloneEnv(env), nil
}

// transitionLocked validates and queues Start/Stop/Delete, idempotently.
func (o *Orchestrator) transitionLocked(kind model.OpKind, actor Actor, envID string) error {
	env := o.envLocked(envID)
	if env == nil {
		return errors.New(errors.KindNotFound, "environment not found: "+envID)
	}
	if err := o.authorize(actor, env); err != nil {
		o.log.Warn("transition denied", "envId", envID, "kind", kind, "actor", actor.Name, "reason", err)
		return err
	}
	target := map[model.OpKind]model.Desired{
		model.OpStart:  model.DesiredRunning,
		model.OpStop:   model.DesiredStopped,
		model.OpDelete: model.DesiredDeleted,
	}[kind]

	for _, op := range o.st.Operations {
		if op.EnvID == envID && !op.Done && op.Kind == kind {
			o.log.Info("transition replay: op already queued", "envId", envID, "kind", kind, "op", op.ID)
			return nil
		}
	}
	// Replay no-op only when the OBSERVED phase already satisfies the call.
	// Basing this on phase (not Desired) means a Failed env with
	// Desired=Running still accepts an explicit Start as a retry.
	alreadySatisfied := map[model.OpKind]bool{
		model.OpStart:  env.Phase == model.PhaseRunning,
		model.OpStop:   env.Phase == model.PhaseStopped,
		model.OpDelete: false, // envLocked hides finalized tombstones as NotFound
	}[kind]
	if alreadySatisfied && !o.pendingOpForLocked(envID) {
		o.log.Info("transition replay: already at observed target", "envId", envID, "kind", kind, "phase", env.Phase)
		return nil
	}
	if kind == model.OpStart && env.Phase == model.PhaseExpired {
		return errors.New(errors.KindConflict, "environment "+envID+" is expired; update ttl to renew before starting")
	}
	// Starting a Stopped/Expired env re-acquires its reservation (stopping
	// and expiry released it). Admit atomically under the state lock so
	// concurrent starts cannot over-commit; a Running/Failed env keeps the
	// reservation it already holds.
	if kind == model.OpStart && (env.Phase == model.PhaseStopped || env.Phase == model.PhaseExpired) {
		if ok, reason := o.quota.Fits(o.st, env.Spec.TenantID, env.Spec.Requested()); !ok {
			o.log.Warn("start rejected: quota", "envId", envID, "requested", env.Spec.Requested(), "reason", reason)
			return errors.New(errors.KindQuota, reason)
		}
	}
	op := o.enqueueLocked(kind, envID, actor, nil)
	env.Desired = target
	env.UpdatedAt = o.now()
	if err := o.saveLocked(); err != nil {
		return errors.New(errors.KindInternal, err.Error())
	}
	o.log.Info("transition queued", "envId", envID, "kind", kind, "actor", actor.Name, "op", op.ID)
	return nil
}

func phaseTransitioning(p model.Phase) bool {
	switch p {
	case model.PhasePending, model.PhaseStarting, model.PhaseStopping, model.PhaseDeleting:
		return true
	}
	return false
}

// StartEnvironment queues a desired=Running transition.
func (o *Orchestrator) StartEnvironment(ctx context.Context, actor Actor, envID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.transitionLocked(model.OpStart, actor, envID)
}

// StopEnvironment queues a desired=Stopped transition.
func (o *Orchestrator) StopEnvironment(ctx context.Context, actor Actor, envID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.transitionLocked(model.OpStop, actor, envID)
}

// DeleteEnvironment queues a desired=Deleted tombstone and releases resources.
func (o *Orchestrator) DeleteEnvironment(ctx context.Context, actor Actor, envID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.transitionLocked(model.OpDelete, actor, envID)
}

// GetEnvironment reads one environment; finalized tombstones are NotFound.
func (o *Orchestrator) GetEnvironment(ctx context.Context, actor Actor, envID string) (*model.Environment, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	env := o.envLocked(envID)
	if env == nil {
		return nil, errors.New(errors.KindNotFound, "environment not found: "+envID)
	}
	if err := o.authorize(actor, env); err != nil {
		return nil, err
	}
	return cloneEnv(env), nil
}

// ListEnvironments lists environments visible to the actor: owners see their
// own; admins see all active environments across tenants.
func (o *Orchestrator) ListEnvironments(ctx context.Context, actor Actor) ([]*model.Environment, error) {
	if actor.Name == "" || actor.TenantID == "" {
		return nil, errors.New(errors.KindUnauthorized, "unauthenticated actor")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*model.Environment, 0, len(o.st.Envs))
	for _, e := range o.st.Envs {
		if e.Deleted {
			continue
		}
		visible := actor.Role == RoleAdmin ||
			(actor.Role == RoleOwner && e.Spec.TenantID == actor.TenantID && e.Spec.Owner == actor.Name)
		if visible {
			out = append(out, cloneEnv(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// SetTenantQuota admin-only: atomically replaces a tenant quota.
func (o *Orchestrator) SetTenantQuota(ctx context.Context, actor Actor, tenantID string, q model.Resources) error {
	if err := requireAdmin(actor, "set tenant quota"); err != nil {
		return err
	}
	if tenantID == "" {
		return errors.New(errors.KindValidation, "tenantId is required")
	}
	if err := validateResources(q); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.st.Quotas[tenantID] = &model.TenantQuota{
		TenantID: tenantID, CPUMilli: q.CPUMilli, MemoryMB: q.MemoryMB, StorageGB: q.StorageGB,
		UpdatedAt: o.now(),
	}
	if err := o.saveLocked(); err != nil {
		return errors.New(errors.KindInternal, err.Error())
	}
	o.log.Info("tenant quota set", "tenant", tenantID, "cpu", q.CPUMilli, "memoryMB", q.MemoryMB, "storageGB", q.StorageGB)
	return nil
}

// SetClusterCapacity admin-only: atomically replaces global capacity.
// Shrinking below current usage is allowed (no eviction); further allocation
// is blocked until usage drops.
func (o *Orchestrator) SetClusterCapacity(ctx context.Context, actor Actor, c model.Resources) error {
	if err := requireAdmin(actor, "set cluster capacity"); err != nil {
		return err
	}
	if c.CPUMilli < 0 || c.MemoryMB < 0 || c.StorageGB < 0 {
		return errors.New(errors.KindValidation, "capacity must be non-negative")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.st.Capacity = model.ClusterCapacity{
		CPUMilli: c.CPUMilli, MemoryMB: c.MemoryMB, StorageGB: c.StorageGB, UpdatedAt: o.now(),
	}
	if err := o.saveLocked(); err != nil {
		return errors.New(errors.KindInternal, err.Error())
	}
	o.log.Info("cluster capacity set", "cpu", c.CPUMilli, "memoryMB", c.MemoryMB, "storageGB", c.StorageGB)
	return nil
}

// WaitForIdle blocks until no unfinished operations or transitioning envs
// remain, or ctx fires.
func (o *Orchestrator) WaitForIdle(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		o.mu.Lock()
		idle := o.countPendingLocked() == 0
		if idle {
			for _, e := range o.st.Envs {
				if !e.Deleted && phaseTransitioning(e.Phase) {
					idle = false
					break
				}
			}
		}
		o.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Shutdown stops the reconciliation loop and waits for it to exit.
func (o *Orchestrator) Shutdown(ctx context.Context) error {
	close(o.stopCh)
	select {
	case <-o.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
