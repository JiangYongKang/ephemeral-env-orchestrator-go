// Package backend simulates the resource backend (compute/storage) that the
// orchestrator drives. It is fully local, needs no credentials and models
// flaky behavior deterministically for tests; every operation is idempotent
// keyed on the caller-provided resource id so retries never duplicate work.
package backend

import (
	"context"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// ResourceState is the observed state of a backing resource.
type ResourceState struct {
	ID      string
	Exists  bool
	Running bool
}

// Backend is the resource-plane abstraction.
type Backend interface {
	EnsureCreated(ctx context.Context, id string, res domain.Resources) error
	EnsureStarted(ctx context.Context, id string) error
	EnsureStopped(ctx context.Context, id string) error
	EnsureDeleted(ctx context.Context, id string) error
	Inspect(ctx context.Context, id string) (ResourceState, error)
}

// FakeBackend is an in-memory Backend. Skeleton stub.
type FakeBackend struct{}

// NewFakeBackend constructs an empty fake backend.
func NewFakeBackend() *FakeBackend { return &FakeBackend{} }

func (b *FakeBackend) EnsureCreated(ctx context.Context, id string, res domain.Resources) error {
	return nil
}
func (b *FakeBackend) EnsureStarted(ctx context.Context, id string) error { return nil }
func (b *FakeBackend) EnsureStopped(ctx context.Context, id string) error { return nil }
func (b *FakeBackend) EnsureDeleted(ctx context.Context, id string) error { return nil }
func (b *FakeBackend) Inspect(ctx context.Context, id string) (ResourceState, error) {
	return ResourceState{}, nil
}
