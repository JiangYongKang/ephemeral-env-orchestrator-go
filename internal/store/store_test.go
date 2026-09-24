package store

import (
	"path/filepath"
	"testing"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "state.json")
	fs := NewFileStore(path)
	st, err := fs.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.Envs == nil || st.Dedup == nil {
		t.Fatal("fresh state must be initialized")
	}
	st.Envs["e1"] = &domain.Environment{ID: "e1", Tenant: "t1", Phase: domain.PhaseRunning}
	st.LastSeq = 7
	st.Version = 3
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	got, err := fs.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Envs["e1"].Phase != domain.PhaseRunning || got.LastSeq != 7 || got.Version != 3 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	t.Logf("JUDGE persisted/loaded version=%d lastSeq=%d envPhase=%s", got.Version, got.LastSeq, got.Envs["e1"].Phase)
}

func TestLoadMissingIsEmpty(t *testing.T) {
	fs := NewFileStore(filepath.Join(t.TempDir(), "absent.json"))
	st, err := fs.Load()
	if err != nil || len(st.Envs) != 0 {
		t.Fatalf("missing file must load empty, got %+v err=%v", st, err)
	}
}
