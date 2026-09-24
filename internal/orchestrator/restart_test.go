package orchestrator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/quota"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/resource"
)

// reopenOrch simulates a process restart: same state file, a fresh backend
// (in-memory infra is reattached from durable allocations).
func reopenOrch(t *testing.T, h *testHolds, w *bytes.Buffer) (*Orchestrator, *resource.Backend) {
	t.Helper()
	newBackend := resource.NewBackend()
	o, err := New(h.st, quota.NewChecker(), newBackend,
		WithLogger(newLogger(w)),
		WithClock(h.clock.get),
		WithTickInterval(5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return o, newBackend
}

// TestRestartRecoversRunning restarts while envs are Running; after reopen,
// phases and allocations must be identical with no duplicated resources.
func TestRestartRecoversRunning(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	e1 := createEnv(t, h.o, ownerAlice(), "alpha", 3)
	e2 := createEnv(t, h.o, ownerAlice(), "beta", 2)
	waitIdle(t, h.o)
	h.o.Shutdown(ctx)

	var logs2 bytes.Buffer
	o2, b2 := reopenOrch(t, h, &logs2)
	defer o2.Shutdown(ctx)
	waitIdle(t, o2)

	for _, e := range []struct {
		id string
		n  int
	}{{e1.ID, 3}, {e2.ID, 2}} {
		got, err := o2.GetEnvironment(ctx, ownerAlice(), e.id)
		if err != nil {
			t.Fatalf("get %s after restart: %v", e.id, err)
		}
		if got.Phase != model.PhaseRunning || len(got.Allocations) != e.n {
			t.Fatalf("%s after restart phase=%s allocs=%d want Running/%d", e.id, got.Phase, len(got.Allocations), e.n)
		}
	}
	if st := b2.Stats(); st.Alive != 5 {
		t.Fatalf("reattached backend alive=%d want 5", st.Alive)
	}
	// Idempotency: convergence after restart must not recreate any resource.
	time.Sleep(50 * time.Millisecond)
	if st := b2.Stats(); st.CreateCalls != 0 || st.Alive != 5 {
		t.Fatalf("post-restart drift created resources: %+v", st)
	}
	t.Logf("STATE restart recovered Running 3+2; reattached alive=5 createCalls=0\n%s", firstNLines(logs2.String(), 10))
}

// TestRestartMidCreate: install always-failing create, declare env, restart
// mid-flight, then heal backend after reopen; convergence resumes and never
// duplicates.
func TestRestartMidCreate(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), bigCapacity())
	ctx := context.Background()
	h.backend.SetCreateHook(func(id string, attempt int) error {
		time.Sleep(5 * time.Millisecond)
		return errInfra("infra down before crash")
	})
	env := createEnv(t, h.o, ownerAlice(), "mid", 3)
	time.Sleep(30 * time.Millisecond) // several failed attempts, still Starting
	h.o.Shutdown(ctx)

	// Fresh process: backend healed.
	var logs2 bytes.Buffer
	o2, b2 := reopenOrch(t, h, &logs2)
	defer o2.Shutdown(ctx)
	waitIdle(t, o2)

	got, err := o2.GetEnvironment(ctx, ownerAlice(), env.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != model.PhaseRunning || len(got.Allocations) != 3 {
		t.Fatalf("after restart+heal phase=%s allocs=%d want Running/3", got.Phase, len(got.Allocations))
	}
	if st := b2.Stats(); st.Alive != 3 {
		t.Fatalf("alive=%d want 3 (duplicate after resume?)", st.Alive)
	}
	t.Logf("STATE resumed create across restart -> Running allocs=3 backend=%+v", b2.Stats())
}

// TestRestartMidDelete: restart during delete interruption; release resumes.
func TestRestartMidDelete(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), bigCapacity())
	ctx := context.Background()
	env := createEnv(t, h.o, ownerAlice(), "gone", 4)
	waitIdle(t, h.o)
	// Block deletes, issue delete so phase=Deleting persists with allocs, then crash.
	h.backend.SetDeleteHook(func(id string, attempt int) error { return errInfra("delete down") })
	if err := h.o.DeleteEnvironment(ctx, ownerAlice(), env.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	h.o.Shutdown(ctx)

	o2, b2 := reopenOrch(t, h, &bytes.Buffer{}) // healthy new backend
	defer o2.Shutdown(ctx)
	waitIdle(t, o2)

	if _, err := o2.GetEnvironment(ctx, ownerAlice(), env.ID); err == nil {
		t.Fatal("env should be deleted after resumed teardown")
	}
	if st := b2.Stats(); st.Alive != 0 {
		t.Fatalf("alive=%d want 0 (leak across restart)", st.Alive)
	}
	// Quota restored: a new 4-replica env fits the small quota (1000 cpu).
	createEnv(t, o2, ownerAlice(), "back", 4)
	waitIdle(t, o2)
	if st := b2.Stats(); st.Alive != 4 {
		t.Fatalf("alive=%d want 4 after quota restore", st.Alive)
	}
	t.Logf("STATE delete resumed across restart; no leak; quota restored alive=%d", b2.Stats().Alive)
}

type errInfra string

func (e errInfra) Error() string { return string(e) }
