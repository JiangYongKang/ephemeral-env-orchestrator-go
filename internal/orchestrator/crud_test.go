package orchestrator

import (
	"context"
	"testing"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// TestCRUDHappyPath covers create/get/list/update/stop/start/delete and asserts
// the key state transitions plus quota release.
func TestCRUDHappyPath(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	ctx := context.Background()

	if err := o.SetClusterCapacity(admin(""), res(10, 10, 10)); err != nil {
		t.Fatal(err)
	}
	if err := o.SetTenantLimit(admin(""), "acme", res(5, 5, 5)); err != nil {
		t.Fatal(err)
	}

	e := mustCreate(t, o, CreateRequest{
		Principal: user("acme", "alice"), IdempotencyKey: "k1",
		Name: "web", Resources: res(2, 2, 2), TTL: 0, Start: true,
	})
	t.Logf("INPUT create name=web res={2,2,2} start=true => id=%s phase=%s", e.ID, e.Phase)
	if e.Phase != domain.PhaseRunning || !e.Running {
		t.Fatalf("want running, got %s running=%v", e.Phase, e.Running)
	}
	if c := h.be.Counts(); c.Created != 1 || c.Started != 1 {
		t.Fatalf("side effects: %+v", c)
	}

	// Get by owner and by admin.
	if _, err := o.Get(user("acme", "alice"), e.ID); err != nil {
		t.Fatalf("owner get: %v", err)
	}
	if _, err := o.Get(admin(""), e.ID); err != nil {
		t.Fatalf("admin get: %v", err)
	}

	// List scopes by tenant.
	acme, _ := o.List(user("acme", "bob"))
	other, _ := o.List(user("globex", "x"))
	if len(acme) != 1 || len(other) != 0 {
		t.Fatalf("list isolation acme=%d other=%d", len(acme), len(other))
	}

	// Stop: running -> stopped, quota retained.
	if err := o.Stop(ctx, StopRequest{Principal: user("acme", "alice"), IdempotencyKey: "kstop", EnvID: e.ID}); err != nil {
		t.Fatal(err)
	}
	got, _ := o.Get(user("acme", "alice"), e.ID)
	t.Logf("TRANSITION stop => phase=%s reserved=%+v (quota must be retained while stopped)", got.Phase, got.Reserved)
	if got.Phase != domain.PhaseStopped || got.Reserved != res(2, 2, 2) {
		t.Fatalf("stop mismatch phase=%s reserved=%+v", got.Phase, got.Reserved)
	}

	// Start again: no duplicate create side effect.
	if err := o.Start(ctx, StartRequest{Principal: user("acme", "alice"), IdempotencyKey: "kstart", EnvID: e.ID}); err != nil {
		t.Fatal(err)
	}
	if c := h.be.Counts(); c.Created != 1 {
		t.Fatalf("start must not recreate backing resource: %+v", c)
	}

	// Update name + shrink resources.
	upd, err := o.Update(ctx, UpdateRequest{
		Principal: user("acme", "alice"), IdempotencyKey: "kupd", EnvID: e.ID,
		Name: strp("web2"), Resources: rp(res(1, 1, 1)),
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if upd.Spec.Name != "web2" || upd.Reserved != res(1, 1, 1) {
		t.Fatalf("update not applied: %+v", upd)
	}

	// Delete releases quota and removes backing resource.
	used, _ := o.TenantUsage(admin(""), "acme")
	if used != res(1, 1, 1) {
		t.Fatalf("usage before delete: %+v", used)
	}
	if err := o.Delete(ctx, DeleteRequest{Principal: user("acme", "alice"), IdempotencyKey: "kdel", EnvID: e.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, _ = o.Get(user("acme", "alice"), e.ID)
	t.Logf("TRANSITION delete => phase=%s reserved=%+v", got.Phase, got.Reserved)
	if got.Phase != domain.PhaseTerminated || got.Reserved != (domain.Resources{}) {
		t.Fatalf("delete mismatch phase=%s reserved=%+v", got.Phase, got.Reserved)
	}
	if len(h.be.Existing()) != 0 {
		t.Fatalf("leaked resources: %v", h.be.Existing())
	}
	if u, _ := o.TenantUsage(admin(""), "acme"); u != (domain.Resources{}) {
		t.Fatalf("quota not released: %+v", u)
	}
}

// TestIdempotentDuplicateCreate repeats the same key and asserts a single
// environment and a single set of backend side effects.
func TestIdempotentDuplicateCreate(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	req := CreateRequest{
		Principal: user("acme", "alice"), IdempotencyKey: "same-key",
		Name: "dup", Resources: res(1, 1, 1), Start: true,
	}
	e1 := mustCreate(t, o, req)
	e2 := mustCreate(t, o, req)
	e3 := mustCreate(t, o, req)
	t.Logf("INPUT 3x create same key => ids %s %s %s", e1.ID, e2.ID, e3.ID)
	if e1.ID != e2.ID || e2.ID != e3.ID {
		t.Fatalf("duplicate submission produced extra environments")
	}
	if c := h.be.Counts(); c.Created != 1 || c.Started != 1 {
		t.Fatalf("duplicate side effects: %+v", c)
	}
	if list, _ := o.List(user("acme", "alice")); len(list) != 1 {
		t.Fatalf("expected 1 env, got %d", len(list))
	}
}

// TestIdempotentFailedReplay replays a rejected submission; it must return the
// same classified error and create nothing.
func TestIdempotentFailedReplay(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	if err := o.SetClusterCapacity(admin(""), res(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	req := CreateRequest{
		Principal: user("acme", "alice"), IdempotencyKey: "too-big",
		Name: "big", Resources: res(5, 5, 5), Start: true,
	}
	_, err1 := o.Create(context.Background(), req)
	_, err2 := o.Create(context.Background(), req)
	t.Logf("INPUT 2x over-quota create => err1=%v err2=%v", err1, err2)
	if !domain.IsKind(err1, domain.KindQuota) {
		t.Fatalf("want quota error, got %v", err1)
	}
	if domain.AsError(err2).Kind != domain.AsError(err1).Kind {
		t.Fatalf("replay error classification differs: %v vs %v", err1, err2)
	}
	if list, _ := o.List(user("acme", "alice")); len(list) != 0 {
		t.Fatalf("rejected create must not leave an environment")
	}
}

func strp(s string) *string                   { return &s }
func rp(r domain.Resources) *domain.Resources { return &r }
