package store

import (
	"path/filepath"
	"testing"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

func TestLoadMissingReturnsEmpty(t *testing.T) {
	st, err := New(filepath.Join(t.TempDir(), "nested", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := st.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Envs) != 0 || got.Version != 1 {
		t.Fatalf("empty load = %+v", got)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	st, err := New(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	in := model.NewState()
	in.Seq = 7
	in.Envs["env-1"] = &model.Environment{
		ID: "env-1", Phase: model.PhaseRunning, Generation: 2,
		Spec: model.EnvSpec{
			Name: "web", Owner: "alice", TenantID: "t1", Image: "img",
			Replicas: 2, Resources: model.Resources{CPUMilli: 50, MemoryMB: 64, StorageGB: 1},
		},
		Allocations: []model.Allocation{{ID: "env-1-g2-0", EnvID: "env-1"}},
	}
	in.Quotas["t1"] = &model.TenantQuota{TenantID: "t1", CPUMilli: 1000, MemoryMB: 1024, StorageGB: 10}
	if err := st.Save(in); err != nil {
		t.Fatal(err)
	}
	out, err := st.Load()
	if err != nil {
		t.Fatal(err)
	}
	if out.Seq != 7 {
		t.Fatalf("seq=%d want 7", out.Seq)
	}
	e := out.Envs["env-1"]
	if e == nil || e.Phase != model.PhaseRunning || e.Generation != 2 || len(e.Allocations) != 1 {
		t.Fatalf("env roundtrip mismatch: %+v", e)
	}
	if out.Quotas["t1"].CPUMilli != 1000 {
		t.Fatal("quota lost in roundtrip")
	}
	t.Logf("ROUNDTRIP seq=%d env.phase=%s gen=%d alloc=%s quota.cpu=%d",
		out.Seq, e.Phase, e.Generation, e.Allocations[0].ID, out.Quotas["t1"].CPUMilli)
}

func TestSaveCreatesNoTempLeftovers(t *testing.T) {
	dir := t.TempDir()
	st, _ := New(filepath.Join(dir, "state.json"))
	if err := st.Save(model.NewState()); err != nil {
		t.Fatal(err)
	}
	entries, err := filepath.Glob(filepath.Join(dir, ".state-*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temp files leaked: %v", entries)
	}
}
