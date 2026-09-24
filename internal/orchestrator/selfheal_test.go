package orchestrator

import (
	"context"
	"testing"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// TestSelfHealVanishedResource simulates the resource plane losing a backing
// resource out-of-band (e.g. manual cleanup). Convergence must detect it via
// Inspect, recreate exactly one resource and return to running, without
// duplicating the environment or its quota reservation.
func TestSelfHealVanishedResource(t *testing.T) {
	h := newHarness(t, backend.NewFakeBackend())
	defer h.o.Close()
	o := h.o
	p := user("acme", "alice")
	e := mustCreate(t, o, CreateRequest{
		Principal: p, IdempotencyKey: "h", Name: "heal",
		Resources: res(2, 2, 2), Start: true,
	})
	if c := h.be.Counts(); c.Created != 1 {
		t.Fatalf("setup create count: %+v", c)
	}
	// Out-of-band deletion of the backing resource.
	h.be.ForceDeleteForTest("env-" + string(e.ID))
	if len(h.be.Existing()) != 0 {
		t.Fatalf("test precondition: resource should be gone")
	}
	t.Logf("STATE backing resource vanished out-of-band while env phase=%s", e.Phase)

	if err := o.TriggerReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := o.Get(p, e.ID)
	if got.Phase != domain.PhaseRunning {
		t.Fatalf("expected self-heal back to running, got %s", got.Phase)
	}
	c := h.be.Counts()
	if c.Created != 2 {
		t.Fatalf("expected a recreate (CreatedN=2: original + healed), got %+v", c)
	}
	if len(h.be.Existing()) != 1 {
		t.Fatalf("expected exactly one live resource after heal, got %v", h.be.Existing())
	}
	if got.Reserved != res(2, 2, 2) {
		t.Fatalf("reservation must remain intact after heal: %+v", got.Reserved)
	}
	t.Logf("JUDGE self-heal recreated one resource -> phase=%s CreatedN=%d live=%d reservation held",
		got.Phase, c.Created, len(h.be.Existing()))
}
