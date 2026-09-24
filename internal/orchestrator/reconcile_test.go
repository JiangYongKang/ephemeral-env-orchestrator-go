package orchestrator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/resource"
)

// TestTransientCreateRetries: first 2 create attempts per allocation fail,
// then succeed. The env must converge with exactly replicas allocations and
// no duplicates (backend dedup also asserted).
func TestTransientCreateRetries(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	defer h.o.Shutdown(context.Background())
	h.backend.SetCreateHook(resource.FailFirstTimes(2, "simulated network blip"))

	env := createEnv(t, h.o, ownerAlice(), "flaky", 3)
	waitIdle(t, h.o)

	got, _ := h.o.GetEnvironment(context.Background(), ownerAlice(), env.ID)
	if got.Phase != model.PhaseRunning {
		t.Fatalf("phase=%s want Running", got.Phase)
	}
	if len(got.Allocations) != 3 {
		t.Fatalf("allocs=%d want 3", len(got.Allocations))
	}
	if st := h.backend.Stats(); st.Alive != 3 {
		t.Fatalf("backend alive=%d want 3 (duplicate resource?)", st.Alive)
	}
	t.Logf("STATE transient failures retried -> Running allocs=3 backend=%+v", h.backend.Stats())
}

// TestPersistentCreateFailure reaches the Failed terminal with a reason and
// releases nothing; an explicit start after clearing the fault recovers.
func TestPersistentCreateFailureAndRecover(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	defer h.o.Shutdown(context.Background())
	h.backend.SetCreateHook(resource.FailAlways("simulated disk full"))

	env := createEnv(t, h.o, ownerAlice(), "hard", 2)
	// wait for the env to enter Failed (attempts capped) rather than idle.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		g, _ := h.o.GetEnvironment(context.Background(), ownerAlice(), env.ID)
		if g.Phase == model.PhaseFailed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("never failed, phase=%s reason=%q", g.Phase, g.FailReason)
		default:
		}
		timeSleep10()
	}
	g, _ := h.o.GetEnvironment(context.Background(), ownerAlice(), env.ID)
	if g.FailReason == "" {
		t.Fatal("failed env must carry explainable FailReason")
	}
	t.Logf("STATE hard failure -> Failed reason=%q allocs=%d", g.FailReason, len(g.Allocations))
	if len(g.Allocations) != 0 {
		t.Fatalf("hard-failed create should reserve nothing live, got %d", len(g.Allocations))
	}

	// Heal the backend and explicitly start; convergence must recover.
	h.backend.SetCreateHook(nil)
	if err := h.o.StartEnvironment(context.Background(), ownerAlice(), env.ID); err != nil {
		t.Fatalf("start after heal: %v", err)
	}
	waitIdle(t, h.o)
	g, _ = h.o.GetEnvironment(context.Background(), ownerAlice(), env.ID)
	if g.Phase != model.PhaseRunning || len(g.Allocations) != 2 {
		t.Fatalf("recovered phase=%s allocs=%d", g.Phase, len(g.Allocations))
	}
	t.Logf("STATE explicit retry -> Running phase=%s allocs=2\n%s", g.Phase, firstNLines(logs.String(), 12))
}

// TestDeleteInterruptedRelease: deletes fail the first time; the delete must
// resume across ticks and never leak an allocation.
func TestDeleteInterruptedRelease(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	defer h.o.Shutdown(context.Background())
	env := createEnv(t, h.o, ownerAlice(), "rm", 3)
	waitIdle(t, h.o)
	h.backend.SetDeleteHook(resource.FailFirstTimes(1, "simulated delete blip"))

	if err := h.o.DeleteEnvironment(context.Background(), ownerAlice(), env.ID); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, h.o)
	if st := h.backend.Stats(); st.Alive != 0 {
		t.Fatalf("alive=%d want 0 (leak after interrupted delete)", st.Alive)
	}
	if _, err := h.o.GetEnvironment(context.Background(), ownerAlice(), env.ID); err == nil {
		t.Fatal("deleted env must not be readable")
	}
	// quota reusable
	createEnv(t, h.o, ownerAlice(), "reborn", 3)
	waitIdle(t, h.o)
	if st := h.backend.Stats(); st.Alive != 3 {
		t.Fatalf("alive after reuse=%d want 3", st.Alive)
	}
	t.Logf("STATE interrupted delete resumed; no leak; quota reused alive=%d", h.backend.Stats().Alive)
}

// TestOperationPriority enqueues start then stop then delete "simultaneously"
// for one env; delete (highest priority) must win and release everything.
func TestOperationPriority(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	defer h.o.Shutdown(context.Background())
	env := createEnv(t, h.o, ownerAlice(), "prio", 2)
	waitIdle(t, h.o)

	// Enqueue without waiting: start, stop, delete. Deterministic priority
	// ordering (delete < stop < start by priority, FIFO tiebreak) decides.
	ctx := context.Background()
	a := ownerAlice()
	if err := h.o.StartEnvironment(ctx, a, env.ID); err != nil { // idempotent no-op (already running)
		t.Logf("start no-op: %v", err)
	}
	if err := h.o.StopEnvironment(ctx, a, env.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.o.DeleteEnvironment(ctx, a, env.ID); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, h.o)
	if _, err := h.o.GetEnvironment(ctx, a, env.ID); err == nil {
		t.Fatal("delete must win over concurrent stop")
	}
	if st := h.backend.Stats(); st.Alive != 0 {
		t.Fatalf("alive=%d want 0", st.Alive)
	}
	t.Logf("STATE conflicting ops resolved by priority -> Deleted alive=0\n%s", firstNLines(logs.String(), 20))
}

// TestPriorityDeleteBeatsCreate uses a brand-new env: a delete enqueued while
// create is pending supersedes the create and leaves no resources.
func TestPriorityDeleteBeatsCreate(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), bigCapacity())
	defer h.o.Shutdown(context.Background())
	// Slow creates down so delete can overtake the create in the queue.
	h.backend.SetCreateHook(func(id string, attempt int) error {
		timeSleep50()
		return nil
	})
	ctx := context.Background()
	env := createEnv(t, h.o, ownerAlice(), "race", 5)
	// Immediately request delete while create is still converging.
	if err := h.o.DeleteEnvironment(ctx, ownerAlice(), env.ID); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, h.o)
	if _, err := h.o.GetEnvironment(ctx, ownerAlice(), env.ID); err == nil {
		t.Fatal("expected deleted")
	}
	if st := h.backend.Stats(); st.Alive != 0 {
		t.Fatalf("alive=%d want 0; create superseded by delete must release everything", st.Alive)
	}
	t.Logf("STATE delete superseded in-flight create; allocs released alive=%d", h.backend.Stats().Alive)
}

var _ = model.PhasePending
