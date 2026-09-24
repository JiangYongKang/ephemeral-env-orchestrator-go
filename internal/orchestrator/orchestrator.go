// Package orchestrator is the application facade: it validates declarations,
// authorizes callers, settles quota, persists intent and triggers
// reconciliation. All decision making happens under a single state mutex and
// every mutation is fsynced to disk before returning acceptance, which is what
// makes restarts recover a consistent result.
package orchestrator

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/quota"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/reconciler"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/store"
)

// Config parameterizes a new Orchestrator.
type Config struct {
	StorePath string
	Clock     domain.Clock
	Logger    *log.Logger
	Backend   backend.Backend
}

// Orchestrator is the main entry point of the control plane.
type Orchestrator struct {
	mu  sync.Mutex // guards st
	st  *domain.State
	fs  *store.FileStore
	qm  *quota.Manager
	rec *reconciler.Reconciler
	be  backend.Backend
	clk domain.Clock
	log *log.Logger
}

// New loads persisted state (if any) and returns a ready orchestrator.
func New(ctx context.Context, cfg Config) (*Orchestrator, error) { return nil, nil }

// CreateRequest is a declarative create submission.
type CreateRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	Name           string
	Resources      domain.Resources
	TTL            time.Duration
	Start          bool
}

// UpdateRequest mutates the mutable parts of a declaration.
type UpdateRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	EnvID          domain.EnvID
	Name           *string
	Resources      *domain.Resources
	TTL            *time.Duration
	DesiredRunning *bool
}

// Get returns a snapshot of one environment.
func (o *Orchestrator) Get(p domain.Principal, id domain.EnvID) (*domain.Environment, error) {
	return nil, nil
}

// List returns all environments visible to the principal.
func (o *Orchestrator) List(p domain.Principal) ([]*domain.Environment, error) { return nil, nil }

// Create accepts (and reconciles) a new environment declaration.
func (o *Orchestrator) Create(ctx context.Context, req CreateRequest) (*domain.Environment, error) {
	return nil, nil
}

// Update accepts a declaration update.
func (o *Orchestrator) Update(ctx context.Context, req UpdateRequest) (*domain.Environment, error) {
	return nil, nil
}

// StartRequest issues a start intent.
type StartRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	EnvID          domain.EnvID
}

// StopRequest issues a stop intent.
type StopRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	EnvID          domain.EnvID
}

// DeleteRequest issues a delete intent.
type DeleteRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	EnvID          domain.EnvID
}

func (o *Orchestrator) Start(ctx context.Context, req StartRequest) error   { return nil }
func (o *Orchestrator) Stop(ctx context.Context, req StopRequest) error     { return nil }
func (o *Orchestrator) Delete(ctx context.Context, req DeleteRequest) error { return nil }

// TriggerReconcile runs one convergence pump synchronously.
func (o *Orchestrator) TriggerReconcile(ctx context.Context) error { return nil }

// SetTenantLimit configures the quota limit for a tenant (admin only).
func (o *Orchestrator) SetTenantLimit(p domain.Principal, t domain.TenantID, lim domain.Resources) error {
	return nil
}

// SetClusterCapacity configures the global capacity (admin only).
func (o *Orchestrator) SetClusterCapacity(p domain.Principal, lim domain.Resources) error { return nil }
