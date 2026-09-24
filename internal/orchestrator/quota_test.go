package orchestrator

import (
	"context"
	"sync"
	"testing"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// TestTenantQuotaBoundary: a tenant limit and global capacity are enforced with
// distinguishable rejections; the exact fit is allowed.
func TestTenantQuotaBoundary(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	if err := o.SetTenantLimit(admin(""), "acme", res(3, 3, 3)); err != nil {
		t.Fatal(err)
	}
	if err := o.SetClusterCapacity(admin(""), res(100, 100, 100)); err != nil {
		t.Fatal(err)
	}
	p := user("acme", "alice")

	// First request fits exactly (3/3/3).
	e1 := mustCreate(t, o, CreateRequest{Principal: p, IdempotencyKey: "q1", Name: "n1", Resources: res(3, 3, 3)})
	t.Logf("INPUT request exact fit 3/3/3 => phase=%s", e1.Phase)

	// Anything more must be rejected as tenant_quota.
	_, err := o.Create(context.Background(), CreateRequest{Principal: p, IdempotencyKey: "q2", Name: "n2", Resources: res(1, 1, 1)})
	de := domain.AsError(err)
	if de == nil || de.Kind != domain.KindQuota || de.Distinct != "tenant_quota" {
		t.Fatalf("want quota/tenant_quota, got %v", err)
	}
	t.Logf("JUDGE over tenant limit => %s/%s", de.Kind, de.Distinct)

	// Stopping does not release quota.
	if err := o.Stop(context.Background(), StopRequest{Principal: p, EnvID: e1.ID}); err != nil {
		t.Fatal(err)
	}
	if u, _ := o.TenantUsage(admin(""), "acme"); u != res(3, 3, 3) {
		t.Fatalf("stopped env must still hold quota, usage=%+v", u)
	}

	// Delete releases, then the same request fits again.
	if err := o.Delete(context.Background(), DeleteRequest{Principal: p, EnvID: e1.ID}); err != nil {
		t.Fatal(err)
	}
	if u, _ := o.TenantUsage(admin(""), "acme"); u != (domain.Resources{}) {
		t.Fatalf("usage after delete: %+v", u)
	}
	mustCreate(t, o, CreateRequest{Principal: p, IdempotencyKey: "q3", Name: "n3", Resources: res(3, 3, 3)})
	t.Logf("JUDGE quota released after delete and capacity reusable")
}

// TestClusterCapacityDistinct: with no tenant limit but a tight global cap, the
// rejection distinct code is cluster_capacity, not tenant_quota.
func TestClusterCapacityDistinct(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	if err := o.SetClusterCapacity(admin(""), res(2, 2, 2)); err != nil {
		t.Fatal(err)
	}
	_ = mustCreate(t, o, CreateRequest{Principal: user("a", "u1"), IdempotencyKey: "c1", Name: "x", Resources: res(2, 2, 2)})
	_, err := o.Create(context.Background(), CreateRequest{Principal: user("b", "u2"), IdempotencyKey: "c2", Name: "y", Resources: res(1, 1, 1)})
	de := domain.AsError(err)
	if de == nil || de.Kind != domain.KindQuota || de.Distinct != "cluster_capacity" {
		t.Fatalf("want quota/cluster_capacity, got %v", err)
	}
	t.Logf("JUDGE global oversubscription => %s/%s", de.Kind, de.Distinct)
}

// TestConcurrentQuotaNoOvercommit hammers create from many goroutines against
// a capacity that fits only a subset, asserting no oversubscription and that
// accepted + rejected counts are consistent and no duplicate resources exist.
func TestConcurrentQuotaNoOvercommit(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	const N = 40
	if err := o.SetClusterCapacity(admin(""), res(10, 10, 10)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, rejected := 0, 0
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := user("acme", "alice")
			_, err := o.Create(context.Background(), CreateRequest{
				Principal: p,
				// Distinct key per goroutine => distinct submissions.
				IdempotencyKey: "cc-" + itoa(i),
				Name:           "n" + itoa(i),
				Resources:      res(1, 1, 1),
				Start:          true,
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				accepted++
			} else if domain.IsKind(err, domain.KindQuota) {
				rejected++
			} else {
				t.Errorf("unexpected error on %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	used, _ := o.TenantUsage(admin(""), "acme")
	t.Logf("INPUT %d concurrent requests capacity=10 => accepted=%d rejected=%d usage=%+v backendCreated=%d",
		N, accepted, rejected, used, h.be.Counts().Created)
	if accepted != 10 || rejected != 30 {
		t.Fatalf("expected 10 accepted / 30 rejected, got %d/%d", accepted, rejected)
	}
	if used != res(10, 10, 10) {
		t.Fatalf("over/under-subscription: usage=%+v", used)
	}
	if c := h.be.Counts(); c.Created != 10 {
		t.Fatalf("expected exactly 10 backing resources, got %d", c.Created)
	}
	if len(h.be.Existing()) != 10 {
		t.Fatalf("expected 10 live resources, got %d", len(h.be.Existing()))
	}
}

// TestConcurrentSameIdempotencyKey fires the same key concurrently; exactly one
// environment may result.
func TestConcurrentSameIdempotencyKey(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	if err := o.SetClusterCapacity(admin(""), res(100, 100, 100)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := o.Create(context.Background(), CreateRequest{
				Principal:      user("acme", "alice"),
				IdempotencyKey: "single",
				Name:           "only",
				Resources:      res(1, 1, 1),
				Start:          true,
			})
			if err != nil {
				errs <- err
				return
			}
			ids <- string(e.ID)
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	first := ""
	n := 0
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("divergent env ids under same key: %s vs %s", first, id)
		}
		n++
	}
	for err := range errs {
		t.Errorf("unexpected duplicate-key error: %v", err)
	}
	if h.be.Counts().Created != 1 {
		t.Fatalf("expected 1 created backing resource, got %d", h.be.Counts().Created)
	}
	t.Logf("JUDGE 20 concurrent same-key requests -> 1 env %s, 1 backing resource (success responses=%d)", first, n)
}

// TestShrinkCapacityGuarded ensures capacity cannot be lowered below usage.
func TestShrinkCapacityGuarded(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	if err := o.SetClusterCapacity(admin(""), res(5, 5, 5)); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, o, CreateRequest{Principal: user("a", "u"), IdempotencyKey: "s", Name: "s", Resources: res(3, 3, 3)})
	if err := o.SetClusterCapacity(admin(""), res(2, 2, 2)); !domain.IsKind(err, domain.KindQuota) {
		t.Fatalf("shrink below usage must be rejected, got %v", err)
	}
	if err := o.SetClusterCapacity(admin(""), res(3, 3, 3)); err != nil {
		t.Fatalf("shrink to exactly usage must be allowed, got %v", err)
	}
	t.Logf("JUDGE capacity shrink below in-use rejected; shrink to exact usage allowed")
}

// TestUpdateQuotaDelta checks growth beyond quota is rejected atomically and
// that a rejected update leaves the original reservation intact.
func TestUpdateQuotaDelta(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	if err := o.SetTenantLimit(admin(""), "acme", res(4, 4, 4)); err != nil {
		t.Fatal(err)
	}
	if err := o.SetClusterCapacity(admin(""), res(100, 100, 100)); err != nil {
		t.Fatal(err)
	}
	p := user("acme", "alice")
	e := mustCreate(t, o, CreateRequest{Principal: p, IdempotencyKey: "u1", Name: "u", Resources: res(2, 2, 2), Start: true})

	// Grow to 4: allowed.
	if _, err := o.Update(context.Background(), UpdateRequest{
		Principal: p, IdempotencyKey: "u2", EnvID: e.ID, Resources: rp(res(4, 4, 4)),
	}); err != nil {
		t.Fatalf("grow to limit should succeed: %v", err)
	}
	// Grow to 5: rejected, reservation must stay at 4.
	_, err := o.Update(context.Background(), UpdateRequest{
		Principal: p, IdempotencyKey: "u3", EnvID: e.ID, Resources: rp(res(5, 5, 5)),
	})
	if !domain.IsKind(err, domain.KindQuota) {
		t.Fatalf("growth beyond quota rejected, got %v", err)
	}
	got, _ := o.Get(p, e.ID)
	if got.Reserved != res(4, 4, 4) || got.Spec.Requested != res(4, 4, 4) {
		t.Fatalf("rejected update left state changed: reserved=%+v requested=%+v", got.Reserved, got.Spec.Requested)
	}
	if u, _ := o.TenantUsage(admin(""), "acme"); u != res(4, 4, 4) {
		t.Fatalf("usage drifted after rejected update: %+v", u)
	}
	t.Logf("JUDGE rejected growth kept reservation at 4/4/4 atomically")
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
