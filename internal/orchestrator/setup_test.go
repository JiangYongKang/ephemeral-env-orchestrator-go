package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/quota"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/resource"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/store"
)

// testHolds bundles a constructed orchestrator with its clock and backend.
type testHolds struct {
	o       *Orchestrator
	st      *store.Store
	backend *resource.Backend
	clock   *atomicClock
}

type atomicClock struct{ v atomic.Int64 } // unix nanos

func newAtomicClock(t time.Time) *atomicClock {
	c := &atomicClock{}
	c.v.Store(t.UnixNano())
	return c
}
func (c *atomicClock) get() time.Time { return time.Unix(0, c.v.Load()) }
func (c *atomicClock) advance(d time.Duration) {
	c.v.Add(int64(d))
}

func newLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// newTestOrch builds an isolated orchestrator with a controllable clock and
// fast tick, pre-seeded with one tenant quota and cluster capacity.
func newTestOrch(t *testing.T, w io.Writer, quotaVec, capVec model.Resources) *testHolds {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := store.New(path)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	backend := resource.NewBackend()
	clock := newAtomicClock(time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC))

	o, err := New(st, quota.NewChecker(), backend,
		WithLogger(newLogger(w)),
		WithClock(clock.get),
		WithTickInterval(5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	admin := Actor{Name: "root", TenantID: "admin-tenant", Role: RoleAdmin}
	if err := o.SetClusterCapacity(ctx, admin, capVec); err != nil {
		t.Fatalf("SetClusterCapacity: %v", err)
	}
	if err := o.SetTenantQuota(ctx, admin, "t1", quotaVec); err != nil {
		t.Fatalf("SetTenantQuota: %v", err)
	}
	return &testHolds{o: o, st: st, backend: backend, clock: clock}
}

func ownerAlice() Actor { return Actor{Name: "alice", TenantID: "t1", Role: RoleOwner} }
func ownerBob() Actor   { return Actor{Name: "bob", TenantID: "t1", Role: RoleOwner} }
func admin() Actor      { return Actor{Name: "root", TenantID: "admin-tenant", Role: RoleAdmin} }

func baseResources() model.Resources {
	return model.Resources{CPUMilli: 100, MemoryMB: 128, StorageGB: 1}
}
func smallQuota() model.Resources {
	return model.Resources{CPUMilli: 1000, MemoryMB: 1024, StorageGB: 10}
}
func bigCapacity() model.Resources {
	return model.Resources{CPUMilli: 100000, MemoryMB: 100000, StorageGB: 1000}
}

func waitIdle(t *testing.T, o *Orchestrator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := o.WaitForIdle(ctx); err != nil {
		t.Fatalf("WaitForIdle: %v", err)
	}
}

func createEnv(t *testing.T, o *Orchestrator, actor Actor, name string, replicas int) *model.Environment {
	t.Helper()
	ctx := context.Background()
	env, err := o.CreateEnvironment(ctx, actor, CreateInput{
		Name: name, Image: "img:1", Replicas: replicas,
		Resources: baseResources(), TTLSeconds: 0,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment(%s): %v", name, err)
	}
	return env
}
