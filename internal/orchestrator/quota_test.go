package orchestrator

import (
	"bytes"
	"context"
	"sync"
	"testing"

	cerr "github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// TestQuotaBoundary: quota is exactly 10 replicas of baseResources(100 cpu).
// 10 succeed, the 11th is rejected with QuotaExceeded and produces nothing.
func TestQuotaBoundary(t *testing.T) {
	var logs bytes.Buffer
	// tenant quota: cpu 1000 -> exactly 10 allocations.
	h := newTestOrch(t, &logs, model.Resources{CPUMilli: 1000, MemoryMB: 4096, StorageGB: 100}, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	accepted, rejected := 0, 0
	for i := 0; i < 12; i++ {
		name := "e" + string(rune('a'+i))
		_, err := h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
			Name: name, Image: "img", Replicas: 1, Resources: baseResources(),
		})
		switch {
		case err == nil:
			accepted++
		case cerr.Is(err, cerr.KindQuota):
			rejected++
			t.Logf("REJECT %s -> QuotaExceeded: %v", name, err)
		default:
			t.Fatalf("unexpected err for %s: %v", name, err)
		}
	}
	waitIdle(t, h.o)
	if accepted != 10 || rejected != 2 {
		t.Fatalf("accepted=%d rejected=%d, want 10/2", accepted, rejected)
	}
	if st := h.backend.Stats(); st.Alive != 10 {
		t.Fatalf("alive=%d, want 10", st.Alive)
	}
	t.Logf("BOUNDARY accepted=%d rejected=%d alive=%d", accepted, rejected, h.backend.Stats().Alive)
}

// TestConcurrentQuotaContention fires many creates concurrently; exactly the
// quota's worth may win, with no over-commit and no duplicate resources.
func TestConcurrentQuotaContention(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, model.Resources{CPUMilli: 500, MemoryMB: 4096, StorageGB: 100}, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	const N = 40
	var wg sync.WaitGroup
	var ok, denied int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
				Name: "ce" + pad(i), Image: "img", Replicas: 1, Resources: baseResources(), // 100 cpu each
			})
			mu.Lock()
			if err == nil {
				ok++
			} else if cerr.Is(err, cerr.KindQuota) {
				denied++
			}
			mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()
	waitIdle(t, h.o)

	if ok != 5 || denied != 35 {
		t.Fatalf("ok=%d denied=%d, want 5/35", ok, denied)
	}
	if st := h.backend.Stats(); st.Alive != int(ok) {
		t.Fatalf("alive=%d want %d (no over-commit)", st.Alive, ok)
	}
	list, _ := h.o.ListEnvironments(ctx, ownerAlice())
	if len(list) != 5 {
		t.Fatalf("persisted envs=%d want 5", len(list))
	}
	t.Logf("CONCURRENT contenders=%d accepted=%d rejected=%d alive=%d (atomic admission)\n%s",
		N, ok, denied, h.backend.Stats().Alive, logs.String())
}

func pad(i int) string {
	if i < 10 {
		return "0" + string(rune('0'+i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}

// TestQuotaReleasedOnDelete confirms freed quota is reusable immediately.
func TestQuotaReleasedOnDelete(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, model.Resources{CPUMilli: 200, MemoryMB: 4096, StorageGB: 100}, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	e1 := createEnv(t, h.o, ownerAlice(), "first", 2) // 200 cpu -> full
	waitIdle(t, h.o)
	if _, err := h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
		Name: "second", Image: "img", Replicas: 1, Resources: baseResources(),
	}); !cerr.Is(err, cerr.KindQuota) {
		t.Fatalf("expected quota rejection, got %v", err)
	}
	if err := h.o.DeleteEnvironment(ctx, ownerAlice(), e1.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitIdle(t, h.o)
	// Quota released: a new create must succeed.
	if _, err := h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
		Name: "second", Image: "img", Replicas: 2, Resources: baseResources(),
	}); err != nil {
		t.Fatalf("create after release: %v", err)
	}
	waitIdle(t, h.o)
	if st := h.backend.Stats(); st.Alive != 2 {
		t.Fatalf("alive=%d want 2", st.Alive)
	}
	t.Logf("RELEASE+REUSE deleted env freed 200 cpu; new env admitted; alive=%d", h.backend.Stats().Alive)
}

// TestClusterCapacity rejects when shared capacity (not just tenant quota) binds.
func TestClusterCapacity(t *testing.T) {
	// generous tenant quota, tiny cluster.
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), model.Resources{CPUMilli: 250, MemoryMB: 4096, StorageGB: 100})
	ctx := context.Background()
	defer h.o.Shutdown(ctx)
	createEnv(t, h.o, ownerAlice(), "a", 2) // 200
	waitIdle(t, h.o)
	_, err := h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
		Name: "b", Image: "img", Replicas: 1, Resources: baseResources(), // +100 -> 300 > 250
	})
	if !cerr.Is(err, cerr.KindQuota) {
		t.Fatalf("cluster capacity err=%v, want QuotaExceeded", err)
	}
	t.Logf("CLUSTER capacity rejection: %v", err)
}

// TestResizeQuota covers scaling down (always allowed) and up (rechecked).
func TestResizeQuota(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, model.Resources{CPUMilli: 300, MemoryMB: 4096, StorageGB: 100}, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)
	env := createEnv(t, h.o, ownerAlice(), "rz", 2) // 200 cpu
	waitIdle(t, h.o)

	// Scale up to 4 (400 cpu) exceeds 300 -> rejected at admission.
	four := 4
	if _, err := h.o.UpdateEnvironment(ctx, ownerAlice(), env.ID, UpdateInput{Replicas: &four}); !cerr.Is(err, cerr.KindQuota) {
		t.Fatalf("scale up err=%v want QuotaExceeded", err)
	}
	t.Log("REJECT scale 2->4 beyond quota")

	// Scale down to 1 allowed, converges to one allocation of the new gen.
	one := 1
	if _, err := h.o.UpdateEnvironment(ctx, ownerAlice(), env.ID, UpdateInput{Replicas: &one}); err != nil {
		t.Fatalf("scale down: %v", err)
	}
	waitIdle(t, h.o)
	got, _ := h.o.GetEnvironment(ctx, ownerAlice(), env.ID)
	if got.Phase != model.PhaseRunning || len(got.Allocations) != 1 || got.Generation != 2 {
		t.Fatalf("after scale down phase=%s allocs=%d gen=%d", got.Phase, len(got.Allocations), got.Generation)
	}
	t.Logf("STATE scale 2->1 gen=2 alloc=%s", got.Allocations[0].ID)

	// Now scale up to 3 (300 cpu) fits exactly after releasing one.
	three := 3
	if _, err := h.o.UpdateEnvironment(ctx, ownerAlice(), env.ID, UpdateInput{Replicas: &three}); err != nil {
		t.Fatalf("scale up: %v", err)
	}
	waitIdle(t, h.o)
	got, _ = h.o.GetEnvironment(ctx, ownerAlice(), env.ID)
	if got.Phase != model.PhaseRunning || len(got.Allocations) != 3 || got.Generation != 3 {
		t.Fatalf("after scale up phase=%s allocs=%d gen=%d", got.Phase, len(got.Allocations), got.Generation)
	}
	t.Logf("STATE scale 1->3 gen=3 allocs=%d alive=%d", len(got.Allocations), h.backend.Stats().Alive)
}

// TestStartStoppedRechecksQuota: stopping frees quota; if another env takes
// it, starting the stopped one is rejected until space frees again.
func TestStartStoppedRechecksQuota(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, model.Resources{CPUMilli: 200, MemoryMB: 4096, StorageGB: 100}, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)
	e1 := createEnv(t, h.o, ownerAlice(), "one", 1) // 100
	e2 := createEnv(t, h.o, ownerAlice(), "two", 1) // 100
	waitIdle(t, h.o)
	if err := h.o.StopEnvironment(ctx, ownerAlice(), e1.ID); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, h.o)
	// e2 grows to 2 -> takes the freed 100 cpu.
	two := 2
	if _, err := h.o.UpdateEnvironment(ctx, ownerAlice(), e2.ID, UpdateInput{Replicas: &two}); err != nil {
		t.Fatalf("e2 grow: %v", err)
	}
	waitIdle(t, h.o)
	if err := h.o.StartEnvironment(ctx, ownerAlice(), e1.ID); !cerr.Is(err, cerr.KindQuota) {
		t.Fatalf("start e1 err=%v want QuotaExceeded", err)
	}
	t.Log("REJECT start of stopped env: quota consumed by sibling")
}
