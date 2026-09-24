package orchestrator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// TestTTLExpiryReclaims: an env with TTL=60s moves to Expired once the clock
// passes, releases all resources/quota, and a direct start is rejected with
// Conflict until the TTL is renewed via update.
func TestTTLExpiryReclaims(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, model.Resources{CPUMilli: 200, MemoryMB: 4096, StorageGB: 100}, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	_, err := h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
		Name: "short", Image: "img", Replicas: 2, Resources: baseResources(), TTLSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, h.o)
	if st := h.backend.Stats(); st.Alive != 2 {
		t.Fatalf("alive before expiry=%d want 2", st.Alive)
	}
	t.Logf("INPUT ttl=60s at %s; running allocs=2", h.clock.get().Format(time.RFC3339))

	// Advance past TTL and let convergence notice (IsExpired uses injected clock).
	h.clock.advance(61 * time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		g, _ := h.o.GetEnvironment(ctx, ownerAlice(), "env-1")
		if g != nil && g.Phase == model.PhaseExpired {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := h.o.GetEnvironment(ctx, ownerAlice(), "env-1")
	if got.Phase != model.PhaseExpired || len(got.Allocations) != 0 {
		t.Fatalf("after TTL phase=%s allocs=%d want Expired/0", got.Phase, len(got.Allocations))
	}
	if st := h.backend.Stats(); st.Alive != 0 {
		t.Fatalf("alive after expiry=%d want 0 (quota not reclaimed)", st.Alive)
	}
	t.Logf("STATE clock=%s -> Expired allocations=0 quota reclaimed alive=%d",
		h.clock.get().Format(time.RFC3339), h.backend.Stats().Alive)

	// Direct start is a Conflict.
	err = h.o.StartEnvironment(ctx, ownerAlice(), got.ID)
	if !isKind(err, "Conflict") {
		t.Fatalf("start expired err=%v want Conflict", err)
	}
	t.Logf("DECISION start expired -> Conflict: %v", err)

	// Freed quota is reusable by another env.
	createEnv(t, h.o, ownerAlice(), "filler", 2)
	waitIdle(t, h.o)
	if st := h.backend.Stats(); st.Alive != 2 {
		t.Fatalf("alive=%d want 2 (expired quota reused)", st.Alive)
	}
	t.Log("STATE reclaimed quota reused by new env")

	// Renewing TTL requires quota; with capacity full it is rejected, after
	// deleting filler the renewal succeeds and converges Running.
	ttl := int(120)
	if _, err := h.o.UpdateEnvironment(ctx, ownerAlice(), got.ID, UpdateInput{TTLSeconds: &ttl}); !isKind(err, "QuotaExceeded") {
		t.Fatalf("renew under full quota err=%v want QuotaExceeded", err)
	}
	t.Log("DECISION renewal while quota full -> QuotaExceeded")
}

func isKind(err error, name string) bool {
	if err == nil {
		return false
	}
	return bytes.Contains([]byte(err.Error()), []byte(name+":"))
}

// TestTTLRenewalRestarts verifies update-ttl on an expired env re-runs it.
func TestTTLRenewalRestarts(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)
	env, err := h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
		Name: "ttl2", Image: "img", Replicas: 1, Resources: baseResources(), TTLSeconds: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, h.o)
	h.clock.advance(11 * time.Second)
	waitPhase(t, h.o, env.ID, model.PhaseExpired)

	ttl := int(3600)
	if _, err := h.o.UpdateEnvironment(ctx, ownerAlice(), env.ID, UpdateInput{TTLSeconds: &ttl}); err != nil {
		t.Fatalf("renew: %v", err)
	}
	waitIdle(t, h.o)
	got, _ := h.o.GetEnvironment(ctx, ownerAlice(), env.ID)
	if got.Phase != model.PhaseRunning || len(got.Allocations) != 1 {
		t.Fatalf("renewed phase=%s allocs=%d want Running/1", got.Phase, len(got.Allocations))
	}
	t.Log("STATE renewed expired env -> Running with new expiry")
}

func waitPhase(t *testing.T, o *Orchestrator, id string, want model.Phase) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		g, err := o.GetEnvironment(context.Background(), ownerAlice(), id)
		if err == nil && g.Phase == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("env %s never reached %s", id, want)
}
