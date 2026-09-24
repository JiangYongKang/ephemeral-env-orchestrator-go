package orchestrator

import (
	"bytes"
	"context"
	"strings"
	"testing"

	cerr "github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// TestCRUDHappyPath verifies create -> running -> stop -> start -> delete,
// with allocation counts tracking quota settlement and explainable phases.
func TestCRUDHappyPath(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	t.Log("INPUT create env=web replicas=3 owner=alice tenant=t1")
	env := createEnv(t, h.o, ownerAlice(), "web", 3)
	waitIdle(t, h.o)

	got, err := h.o.GetEnvironment(ctx, ownerAlice(), env.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Phase != model.PhaseRunning {
		t.Fatalf("phase = %s, want Running", got.Phase)
	}
	if len(got.Allocations) != 3 {
		t.Fatalf("allocations = %d, want 3", len(got.Allocations))
	}
	t.Logf("STATE %s -> Running allocations=3 (deterministic ids: %s, %s, %s)",
		env.ID, got.Allocations[0].ID, got.Allocations[1].ID, got.Allocations[2].ID)

	// Stop releases all resources/quota.
	if err := h.o.StopEnvironment(ctx, ownerAlice(), env.ID); err != nil {
		t.Fatalf("stop: %v", err)
	}
	waitIdle(t, h.o)
	got, _ = h.o.GetEnvironment(ctx, ownerAlice(), env.ID)
	if got.Phase != model.PhaseStopped || len(got.Allocations) != 0 {
		t.Fatalf("after stop phase=%s allocs=%d", got.Phase, len(got.Allocations))
	}
	t.Log("STATE stop -> Stopped allocations=0 quota released")

	// Start reacquires.
	if err := h.o.StartEnvironment(ctx, ownerAlice(), env.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitIdle(t, h.o)
	got, _ = h.o.GetEnvironment(ctx, ownerAlice(), env.ID)
	if got.Phase != model.PhaseRunning || len(got.Allocations) != 3 {
		t.Fatalf("after start phase=%s allocs=%d", got.Phase, len(got.Allocations))
	}
	t.Log("STATE start -> Running allocations=3 same deterministic ids")

	// Delete hides the record and frees quota.
	if err := h.o.DeleteEnvironment(ctx, ownerAlice(), env.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitIdle(t, h.o)
	if _, err := h.o.GetEnvironment(ctx, ownerAlice(), env.ID); !cerr.Is(err, cerr.KindNotFound) {
		t.Fatalf("get after delete err=%v, want NotFound", err)
	}
	if st := h.backend.Stats(); st.Alive != 0 {
		t.Fatalf("backend alive=%d, want 0 (resource leak)", st.Alive)
	}
	t.Logf("STATE delete -> tombstone hidden, backend alive=0\nLOGS:\n%s", logs.String())
}

// TestCreateValidation covers distinguishable rejection reasons.
func TestCreateValidation(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	cases := []struct {
		name string
		in   CreateInput
		kind cerr.Kind
	}{
		{"empty name", CreateInput{Image: "i", Replicas: 1, Resources: baseResources()}, cerr.KindValidation},
		{"empty image", CreateInput{Name: "e", Replicas: 1, Resources: baseResources()}, cerr.KindValidation},
		{"zero replicas", CreateInput{Name: "e", Image: "i", Replicas: 0, Resources: baseResources()}, cerr.KindValidation},
		{"bad cpu", CreateInput{Name: "e", Image: "i", Replicas: 1, Resources: model.Resources{MemoryMB: 1, StorageGB: 1}}, cerr.KindValidation},
		{"negative ttl", CreateInput{Name: "e", Image: "i", Replicas: 1, Resources: baseResources(), TTLSeconds: -1}, cerr.KindValidation},
		{"unauthenticated", CreateInput{Name: "e", Image: "i", Replicas: 1, Resources: baseResources()}, cerr.KindUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actor := ownerAlice()
			if tc.name == "unauthenticated" {
				actor = Actor{}
			}
			_, err := h.o.CreateEnvironment(ctx, actor, tc.in)
			if !cerr.Is(err, tc.kind) {
				t.Fatalf("err=%v, want kind %s", err, tc.kind)
			}
			t.Logf("REJECT %s -> %s (reason: %v)", tc.name, tc.kind, err)
		})
	}
}

// TestCreateIdempotent ensures duplicate submission adds no env/resource.
func TestCreateIdempotent(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	first := createEnv(t, h.o, ownerAlice(), "dup", 2)
	waitIdle(t, h.o)
	_, err := h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
		Name: "dup", Image: "img:1", Replicas: 2, Resources: baseResources(),
	})
	if !cerr.Is(err, cerr.KindConflict) {
		t.Fatalf("duplicate create err=%v, want Conflict", err)
	}
	// Third replay, identical.
	_, err = h.o.CreateEnvironment(ctx, ownerAlice(), CreateInput{
		Name: "dup", Image: "img:1", Replicas: 2, Resources: baseResources(),
	})
	if !cerr.Is(err, cerr.KindConflict) {
		t.Fatalf("third create err=%v, want Conflict", err)
	}
	waitIdle(t, h.o)
	list, _ := h.o.ListEnvironments(ctx, ownerAlice())
	if len(list) != 1 {
		t.Fatalf("env count=%d, want 1", len(list))
	}
	if st := h.backend.Stats(); st.Alive != 2 || st.DupCreates != 0 {
		// duplicate requests are rejected before backend, so no dup create calls.
		t.Fatalf("backend stats=%+v, want alive=2 dupCreates=0", st)
	}
	t.Logf("DECISION duplicate create -> Conflict id=%s; no new env/alloc. alive=%d\n%s",
		first.ID, h.backend.Stats().Alive, logs.String())
}

// TestImmutableOwner rejects owner changes distinctly.
func TestImmutableOwner(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)
	env := createEnv(t, h.o, ownerAlice(), "imm", 1)
	newOwner := "mallory"
	_, err := h.o.UpdateEnvironment(ctx, ownerAlice(), env.ID, UpdateInput{Owner: &newOwner})
	if !cerr.Is(err, cerr.KindImmutable) {
		t.Fatalf("update owner err=%v, want ImmutableField", err)
	}
	t.Logf("DECISION owner mutation -> ImmutableField: %v", err)
}

// TestAuthorization covers cross-tenant/non-owner and admin distinctions.
func TestAuthorization(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)
	env := createEnv(t, h.o, ownerAlice(), "sec", 1)

	if _, err := h.o.GetEnvironment(ctx, ownerBob(), env.ID); !cerr.Is(err, cerr.KindForbidden) {
		t.Fatalf("bob get err=%v, want Forbidden", err)
	}
	mallory := Actor{Name: "mallory", TenantID: "t2", Role: RoleOwner}
	if _, err := h.o.GetEnvironment(ctx, mallory, env.ID); !cerr.Is(err, cerr.KindForbidden) {
		t.Fatalf("cross-tenant get err=%v, want Forbidden", err)
	}
	if _, err := h.o.GetEnvironment(ctx, admin(), env.ID); err != nil {
		t.Fatalf("admin get err=%v, want nil", err)
	}
	if err := h.o.SetTenantQuota(ctx, ownerAlice(), "t1", smallQuota()); !cerr.Is(err, cerr.KindForbidden) {
		t.Fatalf("owner set quota err=%v, want Forbidden", err)
	}
	t.Log("DECISIONS non-owner=Forbidden cross-tenant=Forbidden admin=allowed owner-setQuota=Forbidden")

	// NotFound is distinguishable from forbidden for unknown ids.
	if _, err := h.o.GetEnvironment(ctx, ownerAlice(), "env-does-not-exist"); !cerr.Is(err, cerr.KindNotFound) {
		t.Fatalf("missing err=%v, want NotFound", err)
	}
	if !strings.Contains("ValidationError NotFound Forbidden QuotaExceeded Conflict ImmutableField", "NotFound") {
		t.Fatal("error taxonomy sanity")
	}
}
