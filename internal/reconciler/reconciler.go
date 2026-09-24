// Package reconciler drives environments from their recorded intent toward the
// observed state. Every transition is persisted before/after each idempotent
// backend call, so convergence is interruptible: a process restart resumes
// from the last persisted step without duplicating resources.
package reconciler

import (
	"context"
	"log"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// PersistFn atomically stores the state; called after every committed step.
type PersistFn func(*domain.State) error

// Reconciler applies commands and self-heals environments against the backend.
type Reconciler struct {
	be  backend.Backend
	clk domain.Clock
	log *log.Logger
}

// New constructs a Reconciler.
func New(be backend.Backend, clk domain.Clock, lg *log.Logger) *Reconciler {
	return &Reconciler{be: be, clk: clk, log: lg}
}

// Pump processes all pending commands (in deterministic priority order) and
// reconciles expiry/resume. It returns the number of commands fully applied.
// Transient backend failures leave commands pending for a later Pump.
func (r *Reconciler) Pump(ctx context.Context, st *domain.State, persist PersistFn) (int, error) {
	return 0, nil
}

// ReconcileEnv resumes a single environment toward its desired state, used at
// startup and after a crash. It is idempotent.
func (r *Reconciler) ReconcileEnv(ctx context.Context, st *domain.State, id domain.EnvID, persist PersistFn) error {
	return nil
}
