package reconciler

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

func newTestState() *domain.State {
	return &domain.State{
		Envs:            map[domain.EnvID]*domain.Environment{},
		Dedup:           map[string]domain.DedupRecord{},
		TenantLimits:    map[domain.TenantID]domain.Resources{},
		ClusterCapacity: domain.Resources{CPU: 100, Memory: 100, Storage: 100},
	}
}

func TestSmokeProvisionAndDelete(t *testing.T) {
	clk := &domain.FakeClock{T: time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)}
	be := backend.NewFakeBackend()
	lg := log.New(os.Stdout, "[test] ", log.LstdFlags|log.Lmsgprefix)
	rec := New(be, clk, lg)
	st := newTestState()
	persist := func(*domain.State) error { return nil }

	env := &domain.Environment{
		ID: "e1", Tenant: "t1",
		Spec:  domain.Spec{Name: "n", Owner: "u", Requested: domain.Resources{CPU: 2, Memory: 2, Storage: 2}, DesiredRunning: true},
		Phase: domain.PhaseProvisioning,
	}
	st.Envs["e1"] = env

	ctx := context.Background()
	if _, err := rec.Pump(ctx, st, persist); err != nil {
		t.Fatalf("pump: %v", err)
	}
	if env.Phase != domain.PhaseRunning {
		t.Fatalf("want running, got %s", env.Phase)
	}
	if c := be.Counts(); c.Created != 1 || c.Started != 1 {
		t.Fatalf("unexpected side effects: %+v", c)
	}

	// Delete: priority teardown -> stopped -> terminated, quota released.
	st.Commands = append(st.Commands, domain.Command{Seq: 1, EnvID: "e1", Tenant: "t1", Type: domain.CmdDelete})
	if _, err := rec.Pump(ctx, st, persist); err != nil {
		t.Fatalf("pump delete: %v", err)
	}
	if env.Phase != domain.PhaseTerminated {
		t.Fatalf("want terminated, got %s", env.Phase)
	}
	if env.Reserved != (domain.Resources{}) {
		t.Fatalf("quota not released: %+v", env.Reserved)
	}
	if len(be.Existing()) != 0 {
		t.Fatalf("resource leak: %v", be.Existing())
	}
}

func TestSmokeExpiry(t *testing.T) {
	clk := &domain.FakeClock{T: time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)}
	be := backend.NewFakeBackend()
	rec := New(be, clk, log.New(os.Stdout, "[test] ", log.LstdFlags|log.Lmsgprefix))
	st := newTestState()
	exp := clk.T.Add(time.Minute)
	env := &domain.Environment{
		ID: "e2", Tenant: "t1",
		Spec:  domain.Spec{Owner: "u", Requested: domain.Resources{CPU: 1, Memory: 1, Storage: 1}, DesiredRunning: true},
		Phase: domain.PhaseRunning, Reserved: domain.Resources{CPU: 1, Memory: 1, Storage: 1},
		Running: true, BackingIDs: []string{"env-e2"}, ExpiresAt: exp,
	}
	st.Envs["e2"] = env
	persist := func(*domain.State) error { return nil }
	ctx := context.Background()

	if _, err := rec.Pump(ctx, st, persist); err != nil {
		t.Fatal(err)
	}
	if env.Phase != domain.PhaseRunning {
		t.Fatalf("should still run before ttl: %s", env.Phase)
	}
	clk.Advance(2 * time.Minute)
	if _, err := rec.Pump(ctx, st, persist); err != nil {
		t.Fatal(err)
	}
	if env.Phase != domain.PhaseTerminated {
		t.Fatalf("want terminated after ttl, got %s", env.Phase)
	}
}
