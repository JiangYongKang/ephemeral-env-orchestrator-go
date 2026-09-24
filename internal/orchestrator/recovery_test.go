package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// TestRestartResumeCreate: first create hits a transient backend failure and
// returns provisioning; a process restart (same state file, same backend) then
// resumes and reaches running with a single backing resource.
func TestRestartResumeCreate(t *testing.T) {
	be := backend.NewFakeBackend().WithFault(backend.Fault{FailCreateAttempts: []int{1}})
	h := newHarness(t, be)
	defer h.o.Close()
	o := h.o
	ctx := context.Background()

	e, err := o.Create(ctx, CreateRequest{
		Principal: user("acme", "alice"), IdempotencyKey: "c",
		Name: "resume", Resources: res(1, 1, 1), Start: true,
	})
	if err == nil {
		t.Fatal("expected transient create error, got nil")
	}
	if e.Phase != domain.PhaseProvisioning {
		t.Fatalf("want provisioning after wobble, got %s", e.Phase)
	}
	t.Logf("STATE create transient failure => phase=%s pending commands=%d (intent durable)",
		e.Phase, len(h.o.st.Commands))

	// Backend recovers; restart the control-plane process.
	be.WithFault(backend.Fault{})
	o = h.restart(t)
	got, gerr := o.Get(user("acme", "alice"), e.ID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if got.Phase != domain.PhaseRunning {
		t.Fatalf("after restart want running, got %s reason=%s", got.Phase, got.FailureReason)
	}
	if c := be.Counts(); c.Created != 1 || c.Started != 1 {
		t.Fatalf("expected exactly 1 create/start side effect, got %+v", c)
	}
	t.Logf("JUDGE restart resumed provisioning -> %s with one backing resource, no duplication", got.Phase)
}

// TestRestartInterruptedDelete: stop succeeded but delete had not when the
// process died; restart must complete deletion, release quota and leave no
// resource behind.
func TestRestartInterruptedDelete(t *testing.T) {
	t.Run("stop-done-delete-pending", func(t *testing.T) {
		h2 := newHarness(t, backend.NewFakeBackend())
		defer h2.o.Close()
		o := h2.o
		// Running env.
		e := mustCreate(t, o, CreateRequest{
			Principal: user("acme", "alice"), IdempotencyKey: "d",
			Name: "victim", Resources: res(2, 2, 2), Start: true,
		})
		// Make delete fail once after the resource is stopped: delete requires
		// stopped; the first EnsureDeleted attempt transiently fails.
		h2.be.WithFault(backend.Fault{FailDeleteAttempts: []int{1}})
		err := o.Delete(context.Background(), DeleteRequest{
			Principal: user("acme", "alice"), IdempotencyKey: "del", EnvID: e.ID,
		})
		if err == nil {
			t.Fatal("expected transient delete failure")
		}
		mid, _ := o.Get(user("acme", "alice"), e.ID)
		t.Logf("STATE delete interrupted => phase=%s running=%v (stop durable, delete pending)", mid.Phase, mid.Running)
		if mid.Phase != domain.PhaseDeleting {
			t.Fatalf("want deleting, got %s", mid.Phase)
		}
		// Backend heals; restart resumes from persisted deleting state.
		h2.be.WithFault(backend.Fault{})
		o = h2.restart(t)
		got, _ := o.Get(user("acme", "alice"), e.ID)
		if got.Phase != domain.PhaseTerminated {
			t.Fatalf("after restart want terminated, got %s", got.Phase)
		}
		if got.Reserved != (domain.Resources{}) || len(h2.be.Existing()) != 0 {
			t.Fatalf("leak after restart: reserved=%+v existing=%v", got.Reserved, h2.be.Existing())
		}
		if c := h2.be.Counts(); c.Deleted != 1 {
			t.Fatalf("expected exactly 1 delete side effect, got %+v", c)
		}
		t.Logf("JUDGE interrupted delete resumed after restart -> terminated; deletedN=1, no leak")
	})
}

// TestExpiryReclaim creates an env with a short TTL, advances the clock across
// a simulated restart, and verifies the env is terminated and quota released
// without an explicit delete call.
func TestExpiryReclaim(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	if err := o.SetClusterCapacity(admin(""), res(10, 10, 10)); err != nil {
		t.Fatal(err)
	}
	e := mustCreate(t, o, CreateRequest{
		Principal: user("acme", "alice"), IdempotencyKey: "t",
		Name: "short", Resources: res(2, 2, 2), Start: true, TTL: time.Minute,
	})
	t.Logf("INPUT ttl=1m expiresAt=%s now=%s", e.ExpiresAt.Format("15:04:05.000"), h.clk.Now().Format("15:04:05.000"))

	// Before expiry: still running, name still reserved.
	h.clk.Advance(30 * time.Second)
	if err := o.TriggerReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if g, _ := o.Get(user("acme", "alice"), e.ID); g.Phase != domain.PhaseRunning {
		t.Fatalf("must not expire early: %s", g.Phase)
	}

	// Restart happens after TTL; startup pump must reclaim.
	h.clk.Advance(31 * time.Second)
	o = h.restart(t)
	got, _ := o.Get(user("acme", "alice"), e.ID)
	t.Logf("STATE after TTL + restart => phase=%s reserved=%+v existing=%v",
		got.Phase, got.Reserved, h.be.Existing())
	if got.Phase != domain.PhaseTerminated {
		t.Fatalf("want terminated by expiry, got %s", got.Phase)
	}
	if got.Reserved != (domain.Resources{}) || len(h.be.Existing()) != 0 {
		t.Fatalf("expired env leaked resources: reserved=%+v existing=%v", got.Reserved, h.be.Existing())
	}
	// Name becomes reusable and capacity is free.
	mustCreate(t, o, CreateRequest{
		Principal: user("acme", "alice"), IdempotencyKey: "t2",
		Name: "short", Resources: res(2, 2, 2), Start: false,
	})
	t.Logf("JUDGE expiry reclaimed quota, removed resource and freed name")
}

// TestTTLUpdateResetsExpiry verifies updating TTL extends validity and prevents
// reclamation.
func TestTTLUpdateResetsExpiry(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	p := user("acme", "alice")
	e := mustCreate(t, o, CreateRequest{
		Principal: p, IdempotencyKey: "x", Name: "renew", Resources: res(1, 1, 1), Start: true, TTL: time.Minute,
	})
	h.clk.Advance(50 * time.Second)
	// Renew for another 10 minutes from now.
	newTTL := 10 * time.Minute
	if _, err := o.Update(context.Background(), UpdateRequest{
		Principal: p, IdempotencyKey: "renew1", EnvID: e.ID, TTL: &newTTL,
	}); err != nil {
		t.Fatal(err)
	}
	h.clk.Advance(40 * time.Second) // 90s from start, but 40s after renewal
	if err := o.TriggerReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	g, _ := o.Get(p, e.ID)
	if g.Phase.IsTerminal() {
		t.Fatalf("renewed env must not expire, got %s", g.Phase)
	}
	t.Logf("JUDGE TTL renewed, remains %s at 40s after renewal (would have expired under old TTL)", g.Phase)
}
