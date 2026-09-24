package resource

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

func alloc(id string) model.Allocation {
	return model.Allocation{ID: id, EnvID: "env-1", TenantID: "t1", Resources: model.Resources{CPUMilli: 1}}
}

func TestCreateIdempotent(t *testing.T) {
	b := NewBackend()
	ctx := context.Background()
	if err := b.CreateAllocation(ctx, alloc("a")); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateAllocation(ctx, alloc("a")); err != nil { // replay
		t.Fatalf("replay create: %v", err)
	}
	ok, _ := b.HealthExists(ctx, "a")
	if !ok {
		t.Fatal("allocation missing after idempotent create")
	}
	st := b.Stats()
	if st.CreateCalls != 1 || st.DupCreates != 1 || st.Alive != 1 {
		t.Fatalf("stats=%+v want 1 real create, 1 dup, 1 alive", st)
	}
	t.Logf("IDEMPOTENT create stats=%+v (no duplicate resource)", st)
}

func TestDeleteIdempotent(t *testing.T) {
	b := NewBackend()
	ctx := context.Background()
	_ = b.CreateAllocation(ctx, alloc("a"))
	if err := b.DeleteAllocation(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteAllocation(ctx, "a"); err != nil { // already gone
		t.Fatalf("replay delete: %v", err)
	}
	if st := b.Stats(); st.Alive != 0 || st.DupDeletes != 1 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestFailFirstThenSuccess(t *testing.T) {
	b := NewBackend()
	b.SetCreateHook(FailFirstTimes(2, "net"))
	ctx := context.Background()
	var failures int
	for i := 0; i < 4; i++ {
		if err := b.CreateAllocation(ctx, alloc("x")); err != nil {
			failures++
			if !strings.Contains(err.Error(), "attempt") {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	}
	if failures != 2 {
		t.Fatalf("failures=%d want 2", failures)
	}
	if ok, _ := b.HealthExists(ctx, "x"); !ok {
		t.Fatal("should exist after transient failures")
	}
	t.Logf("RETRY first 2 attempts failed, then converged; failures=%d", failures)
}

func TestReattachSurvivesRestart(t *testing.T) {
	b1 := NewBackend()
	ctx := context.Background()
	_ = b1.CreateAllocation(ctx, alloc("p"))
	_ = b1.CreateAllocation(ctx, alloc("q"))

	b2 := NewBackend() // fresh process: infra still exists
	b2.Reattach([]model.Allocation{alloc("p"), alloc("q")})
	if ok, _ := b2.HealthExists(ctx, "p"); !ok {
		t.Fatal("reattached p missing")
	}
	if err := b2.DeleteAllocation(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if st := b2.Stats(); st.Alive != 1 {
		t.Fatalf("after delete one, alive=%d want 1", st.Alive)
	}
	t.Logf("RESTART reattach alive after delete=%d", b2.Stats().Alive)
}

func TestConcurrentCreatesNoDup(t *testing.T) {
	b := NewBackend()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = b.CreateAllocation(ctx, alloc("same")) }()
	}
	wg.Wait()
	if st := b.Stats(); st.Alive != 1 {
		t.Fatalf("alive=%d want exactly 1", st.Alive)
	}
	t.Logf("CONCURRENT 20 creates of same id -> alive=1 stats=%+v", b.Stats())
}

var _ = errors.New
