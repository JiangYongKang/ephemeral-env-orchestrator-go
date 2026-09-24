package quota

import (
	"strings"
	"testing"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

func res(cpu, mem, stor int) model.Resources {
	return model.Resources{CPUMilli: cpu, MemoryMB: mem, StorageGB: stor}
}

func baseState() *model.State {
	st := model.NewState()
	st.Capacity = model.ClusterCapacity{CPUMilli: 1000, MemoryMB: 2048, StorageGB: 50}
	st.Quotas["t1"] = &model.TenantQuota{TenantID: "t1", CPUMilli: 400, MemoryMB: 1024, StorageGB: 20}
	st.Quotas["t2"] = &model.TenantQuota{TenantID: "t2", CPUMilli: 100, MemoryMB: 1024, StorageGB: 20}
	return st
}

func env(id, tenant string, desired model.Desired, spec model.Resources, reps int, live int) *model.Environment {
	e := &model.Environment{
		ID:      id,
		Desired: desired,
		Phase:   model.PhaseRunning,
		Spec: model.EnvSpec{
			Name: id, TenantID: tenant, Replicas: reps, Resources: spec,
		},
	}
	for i := 0; i < live; i++ {
		e.Allocations = append(e.Allocations, model.Allocation{ID: id + "-a" + string(rune('0'+i)), Resources: spec})
	}
	return e
}

func TestFitsAdmitsAndExplains(t *testing.T) {
	c := NewChecker()
	st := baseState()
	st.Envs["e1"] = env("e1", "t1", model.DesiredRunning, res(100, 128, 1), 2, 2) // reserved 200

	if ok, _ := c.Fits(st, "t1", res(200, 128, 1)); !ok { // 200+200=400 fits
		t.Fatal("boundary request should fit exactly")
	}
	ok, reason := c.Fits(st, "t1", res(201, 128, 1))
	if ok {
		t.Fatal("201 cpu should exceed 400 limit")
	}
	if !strings.Contains(reason, "cpu quota exceeded") {
		t.Fatalf("reason should name cpu: %s", reason)
	}
	t.Logf("REJECT tenant boundary: %s", reason)

	if _, reason := c.Fits(st, "unknown", res(1, 1, 1)); !strings.Contains(reason, "no quota configured") {
		t.Fatalf("missing tenant reason: %s", reason)
	}
}

func TestStoppedEnvReservesNothing(t *testing.T) {
	c := NewChecker()
	st := baseState()
	// t1 has a stopped env that previously consumed 400 cpu: reservation 0.
	st.Envs["e1"] = env("e1", "t1", model.DesiredStopped, res(100, 128, 1), 4, 0)
	if got := model.TenantReserved(st, "t1"); got.CPUMilli != 0 {
		t.Fatalf("stopped reservation=%d want 0", got.CPUMilli)
	}
	if ok, _ := c.Fits(st, "t1", res(400, 128, 1)); !ok {
		t.Fatal("stopped env must release its full reservation")
	}
	t.Log("SETTLE stopped env reserves 0; full quota available")
}

func TestClusterCapacityBinds(t *testing.T) {
	c := NewChecker()
	st := baseState()
	st.Quotas["t1"].CPUMilli = 2000                                             // tenant not the binding constraint
	st.Envs["a"] = env("a", "t1", model.DesiredRunning, res(100, 128, 1), 5, 5) // 500
	st.Envs["b"] = env("b", "t2", model.DesiredRunning, res(100, 128, 1), 4, 4) // 400
	// Move t1's 500 reservation -> 700: cluster 1000 binds (400 other + 700).
	ok, reason := c.CheckChange(st, "t1", res(500, 640, 5), res(700, 896, 7))
	if ok {
		t.Fatalf("cluster 1000 cpu: 400(other)+700 should exceed; reason=%s", reason)
	}
	if !strings.Contains(reason, "cluster") {
		t.Fatalf("want cluster reason, got %q", reason)
	}
	t.Logf("REJECT cluster capacity: %s", reason)

	// Shrinking t1's own reservation always passes.
	if ok, _ := c.CheckChange(st, "t1", res(500, 640, 5), res(100, 128, 1)); !ok {
		t.Fatal("shrink should always be admitted")
	}
}

func TestCheckChangeExcludesOwnReservation(t *testing.T) {
	c := NewChecker()
	st := baseState()
	// t1 reserved 400 by one env; moving that same env to 400 again must fit.
	st.Envs["a"] = env("a", "t1", model.DesiredRunning, res(100, 128, 1), 4, 4)
	if ok, reason := c.CheckChange(st, "t1", res(400, 512, 4), res(400, 512, 4)); !ok {
		t.Fatalf("same-size change should fit: %s", reason)
	}
	if ok, reason := c.CheckChange(st, "t1", res(400, 512, 4), res(401, 512, 4)); ok {
		t.Fatalf("growth beyond limit should fail: %s", reason)
	}
}
