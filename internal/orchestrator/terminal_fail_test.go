package orchestrator

import (
	"context"
	"testing"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// TestPermanentCreateFailure: when the resource backend fails non-retryably,
// the environment reaches an explainable failed terminal phase, its reservation
// is released, and a restart does not resurrect it or leak resources.
func TestPermanentCreateFailure(t *testing.T) {
	be := backend.NewFakeBackend().WithFault(backend.Fault{PermanentCreate: true})
	h := newHarness(t, be)
	defer h.o.Close()
	o := h.o
	p := user("acme", "alice")

	e, err := o.Create(context.Background(), CreateRequest{
		Principal: p, IdempotencyKey: "pf", Name: "doomed",
		Resources: res(2, 2, 2), Start: true,
	})
	// The API may return the accepted record; the permanent failure is
	// represented by the environment's terminal failed phase and reason.
	if err != nil && !domain.IsKind(err, domain.KindBackend) {
		t.Fatalf("unexpected create error class: %v", err)
	}
	t.Logf("INPUT create with permanent backend failure => returned err=%v", err)

	got, _ := o.Get(p, e.ID)
	if got.Phase != domain.PhaseFailed {
		t.Fatalf("want failed phase, got %s", got.Phase)
	}
	if got.FailureReason == "" {
		t.Fatalf("failure reason must be explainable")
	}
	if got.Reserved != (domain.Resources{}) {
		t.Fatalf("failed env must release reservation, got %+v", got.Reserved)
	}
	if u, _ := o.TenantUsage(admin(""), "acme"); u != (domain.Resources{}) {
		t.Fatalf("quota leaked on failure: %+v", u)
	}
	if len(h.be.Existing()) != 0 {
		t.Fatalf("no resource should survive: %v", h.be.Existing())
	}
	t.Logf("JUDGE permanent failure -> phase=%s reason=%q quota released", got.Phase, got.FailureReason)

	// Restart must not resurrect a failed env and must not retry forever.
	o = h.restart(t)
	got, _ = o.Get(p, e.ID)
	if got.Phase != domain.PhaseFailed {
		t.Fatalf("failed env must stay failed across restart, got %s", got.Phase)
	}

	// Operations on a failed environment are rejected as terminal.
	if err := o.Start(context.Background(), StartRequest{Principal: p, EnvID: e.ID}); !domain.IsKind(err, domain.KindTerminal) {
		t.Fatalf("start on failed env must be terminal, got %v", err)
	}
}
