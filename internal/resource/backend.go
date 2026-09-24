// Package resource simulates the infrastructure backend that materializes
// and releases resource instances. Failures and flakiness are injectable so
// reconciliation can be exercised without any real cluster, external service
// or credentials.
//
// Idempotency contract:
//   - CreateAllocation is idempotent by allocation ID; a duplicate create is
//     a successful no-op (never produces a second resource).
//   - DeleteAllocation is idempotent; deleting an unknown ID is a no-op.
//
// Restart semantics: the simulated infrastructure outlives the control
// process. Reattach re-registers allocations that were already durable in
// the persisted state, mirroring how real cloud resources survive a
// control-plane restart.
package resource

import (
	"context"
	"fmt"
	"sync"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// Hook decides whether an attempt fails. attempt is 1-based per call target.
type Hook func(id string, attempt int) error

// Backend simulates allocation lifecycle calls with an in-memory alive set.
type Backend struct {
	mu sync.Mutex

	alive map[string]model.Allocation

	createHook Hook
	deleteHook Hook

	createCalls   int
	deleteCalls   int
	dupCreates    int
	dupDeletes    int
	createAttempt map[string]int
	deleteAttempt map[string]int
}

// NewBackend creates an empty simulated backend.
func NewBackend() *Backend {
	return &Backend{
		alive:         map[string]model.Allocation{},
		createAttempt: map[string]int{},
		deleteAttempt: map[string]int{},
	}
}

// SetCreateHook installs a fault hook for create attempts (nil disables).
func (b *Backend) SetCreateHook(h Hook) {
	b.mu.Lock()
	b.createHook = h
	b.mu.Unlock()
}

// SetDeleteHook installs a fault hook for delete attempts (nil disables).
func (b *Backend) SetDeleteHook(h Hook) {
	b.mu.Lock()
	b.deleteHook = h
	b.mu.Unlock()
}

// Reattach registers allocations already materialized before this backend
// instance existed (control-plane restart). No fault hook runs.
func (b *Backend) Reattach(allocs []model.Allocation) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, a := range allocs {
		b.alive[a.ID] = a
	}
}

// CreateAllocation materializes one instance. Idempotent by a.ID.
func (b *Backend) CreateAllocation(ctx context.Context, a model.Allocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	if _, ok := b.alive[a.ID]; ok {
		b.dupCreates++
		b.mu.Unlock()
		return nil // idempotent success, no duplicate resource
	}
	b.createCalls++
	b.createAttempt[a.ID]++
	attempt := b.createAttempt[a.ID]
	hook := b.createHook
	b.mu.Unlock()

	if hook != nil {
		if err := hook(a.ID, attempt); err != nil {
			return err
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.alive[a.ID]; ok { // raced duplicate during unlock window
		b.dupCreates++
		return nil
	}
	b.alive[a.ID] = a
	return nil
}

// DeleteAllocation releases one instance; unknown IDs are a no-op success.
func (b *Backend) DeleteAllocation(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	_, exists := b.alive[id]
	if !exists {
		b.dupDeletes++
		b.mu.Unlock()
		return nil // idempotent release
	}
	b.deleteCalls++
	b.deleteAttempt[id]++
	attempt := b.deleteAttempt[id]
	hook := b.deleteHook
	b.mu.Unlock()

	if hook != nil {
		if err := hook(id, attempt); err != nil {
			return err
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.alive, id)
	return nil
}

// HealthExists reports whether a materialized instance is still present.
func (b *Backend) HealthExists(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.alive[id]
	return ok, nil
}

// Stats summarizes backend calls (useful for test assertions/logging).
type Stats struct {
	CreateCalls int
	DeleteCalls int
	DupCreates  int
	DupDeletes  int
	Alive       int
}

// Stats returns a snapshot of backend counters.
func (b *Backend) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{
		CreateCalls: b.createCalls,
		DeleteCalls: b.deleteCalls,
		DupCreates:  b.dupCreates,
		DupDeletes:  b.dupDeletes,
		Alive:       len(b.alive),
	}
}

// FailFirstTimes builds a hook that fails the first n attempts with the
// given message, then succeeds (models transient blips that must be retried).
func FailFirstTimes(n int, msg string) Hook {
	return func(id string, attempt int) error {
		if attempt <= n {
			return fmt.Errorf("%s (attempt %d for %s)", msg, attempt, id)
		}
		return nil
	}
}

// FailAlways builds a hook that fails every attempt (models a hard failure).
func FailAlways(msg string) Hook {
	return func(id string, attempt int) error {
		return fmt.Errorf("%s (attempt %d)", msg, attempt)
	}
}
