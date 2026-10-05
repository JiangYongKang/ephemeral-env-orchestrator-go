package orchestrator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	cerr "github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// stateFileBytes reads the persisted control-plane document.
func stateFileBytes(t *testing.T, h *testHolds) []byte {
	t.Helper()
	b, err := os.ReadFile(h.st.Path())
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	return b
}

func countEnvs(t *testing.T, o *Orchestrator) int {
	t.Helper()
	envs, err := o.ListEnvironments(context.Background(), admin())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return len(envs)
}

// TestManifestPreviewReadOnly covers scenario: a full manifest (create +
// update + delete + no-op) is previewed twice; the preview must be
// deterministic and must not touch environments, operations, quota or the
// persisted document.
func TestManifestPreviewReadOnly(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	web := createEnv(t, h.o, ownerAlice(), "web", 2)
	waitIdle(t, h.o)
	before := stateFileBytes(t, h)

	manifest := []ManifestItem{
		{Name: "api", Image: "img:1", Replicas: 3, Resources: baseResources()},       // create
		{Name: "web", Image: "img:1", Replicas: 4, Resources: baseResources()},       // update 2->4
		{Name: "ghost", Desired: model.DesiredDeleted},                               // no-op: absent
		{Name: "web", Image: "img:1", Replicas: 5, Resources: baseResources()},       // supersedes item idx1
	}
	plan1, err := h.o.PreviewManifest(ctx, ownerAlice(), manifest)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	plan2, err := h.o.PreviewManifest(ctx, ownerAlice(), manifest)
	if err != nil {
		t.Fatalf("preview again: %v", err)
	}
	if !reflect.DeepEqual(plan1, plan2) {
		t.Fatalf("preview not deterministic:\nfirst=%+v\nsecond=%+v", plan1, plan2)
	}
	if !plan1.Applicable {
		t.Fatalf("plan should be applicable, reject=%s", plan1.RejectReason)
	}

	// Per-item actions and判定依据.
	if got := plan1.Items[0].Action; got != PlanCreate {
		t.Fatalf("item0 action=%s want Create", got)
	}
	if got := plan1.Items[1].Action; got != PlanNoOp {
		t.Fatalf("item1 action=%s want NoOp (superseded by later same-name item)", got)
	}
	if got := plan1.Items[2].Action; got != PlanNoOp {
		t.Fatalf("item2 action=%s want NoOp (delete of absent env)", got)
	}
	if got := plan1.Items[3].Action; got != PlanUpdate {
		t.Fatalf("item3 action=%s want Update", got)
	}
	if plan1.Items[3].EnvID != web.ID {
		t.Fatalf("item3 envID=%s want %s", plan1.Items[3].EnvID, web.ID)
	}
	foundReplicaDiff := false
	for _, d := range plan1.Items[3].Diff {
		t.Logf("DIFF web %s: %s -> %s", d.Field, d.From, d.To)
		if d.Field == "replicas" && d.From == "2" && d.To == "5" {
			foundReplicaDiff = true
		}
	}
	if !foundReplicaDiff {
		t.Fatalf("update diff missing replicas 2->5: %+v", plan1.Items[3].Diff)
	}

	// Net delta: create api (3*100cpu) + grow web by 3 replicas (3*100) = +600m.
	if d := plan1.TenantDelta["t1"]; d.CPUMilli != 600 {
		t.Fatalf("tenant delta cpu=%d want +600", d.CPUMilli)
	}
	if plan1.ClusterDelta.CPUMilli != 600 {
		t.Fatalf("cluster delta cpu=%d want +600", plan1.ClusterDelta.CPUMilli)
	}
	t.Logf("PLAN applicable=%v tenantDelta=%+v clusterDelta=%+v",
		plan1.Applicable, plan1.TenantDelta["t1"], plan1.ClusterDelta)

	// Read-only proof: persisted document and in-memory view unchanged.
	after := stateFileBytes(t, h)
	if !bytes.Equal(before, after) {
		t.Fatalf("preview mutated persisted state")
	}
	if n := countEnvs(t, h.o); n != 1 {
		t.Fatalf("env count after preview=%d want 1", n)
	}
	if got, _ := h.o.GetEnvironment(ctx, ownerAlice(), web.ID); got.Spec.Replicas != 2 {
		t.Fatalf("preview changed spec.replicas to %d", got.Spec.Replicas)
	}
	t.Log("READ-ONLY OK: state file, env records and specs untouched by preview")
}

// TestManifestBatchNetChangeReuse covers scenario: one batch deletes an env
// (releasing 800m cpu) and creates a bigger one (900m). The create alone
// would exceed the remaining quota (200m free), but the batch NET change
// (+100m) fits, so the whole batch must be admitted.
func TestManifestBatchNetChangeReuse(t *testing.T) {
	var logs bytes.Buffer
	// quota: 1000m cpu; "old" reserves 800m.
	h := newTestOrch(t, &logs, model.Resources{CPUMilli: 1000, MemoryMB: 8192, StorageGB: 100}, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	old := createEnv(t, h.o, ownerAlice(), "old", 8)
	waitIdle(t, h.o)

	manifest := []ManifestItem{
		{Name: "old", Desired: model.DesiredDeleted},
		{Name: "new", Image: "img:2", Replicas: 9, Resources: baseResources()},
	}
	plan, err := h.o.PreviewManifest(ctx, ownerAlice(), manifest)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if !plan.Applicable {
		t.Fatalf("net-change batch should fit: %s", plan.RejectReason)
	}
	if plan.ClusterDelta.CPUMilli != 100 {
		t.Fatalf("cluster net delta=%dm want +100m (release 800, take 900)", plan.ClusterDelta.CPUMilli)
	}
	t.Logf("NET-CHANGE preview: delta=%+v reason items=[%s|%s]",
		plan.ClusterDelta, plan.Items[0].Reason, plan.Items[1].Reason)

	res, err := h.o.ApplyManifest(ctx, ownerAlice(), manifest)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !res.Applied {
		t.Fatalf("batch should apply, reject=%s", res.RejectReason)
	}
	waitIdle(t, h.o)

	if _, err := h.o.GetEnvironment(ctx, ownerAlice(), old.ID); !cerr.Is(err, cerr.KindNotFound) {
		t.Fatalf("old env should be deleted, got err=%v", err)
	}
	envs, _ := h.o.ListEnvironments(ctx, ownerAlice())
	if len(envs) != 1 || envs[0].Spec.Name != "new" {
		t.Fatalf("expected only 'new', got %+v", envs)
	}
	if envs[0].Phase != model.PhaseRunning || len(envs[0].Allocations) != 9 {
		t.Fatalf("new env phase=%s allocs=%d want Running/9", envs[0].Phase, len(envs[0].Allocations))
	}
	if st := h.backend.Stats(); st.Alive != 9 {
		t.Fatalf("backend alive=%d want 9 (released allocations reused by the same batch)", st.Alive)
	}
	t.Logf("NET-CHANGE applied: old released, new running with 9 allocations, alive=%d", h.backend.Stats().Alive)
}

// TestManifestBatchOverQuotaRejected covers scenario: the batch net change
// exceeds tenant quota, so the WHOLE batch is rejected with per-item
// QuotaExceeded reasons and zero side effects (no envs, no ops, no state
// drift, no expiry/extension residue).
func TestManifestBatchOverQuotaRejected(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, model.Resources{CPUMilli: 1000, MemoryMB: 8192, StorageGB: 100}, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	keep := createEnv(t, h.o, ownerAlice(), "keep", 5) // 500m reserved
	waitIdle(t, h.o)
	before := stateFileBytes(t, h)

	manifest := []ManifestItem{
		{Name: "a", Image: "img", Replicas: 4, Resources: baseResources()}, // +400m
		{Name: "b", Image: "img", Replicas: 4, Resources: baseResources()}, // +400m -> 1300m > 1000m
	}
	plan, err := h.o.PreviewManifest(ctx, ownerAlice(), manifest)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if plan.Applicable {
		t.Fatalf("plan must be inapplicable when net change exceeds quota")
	}
	t.Logf("OVER-QUOTA preview reject: %s", plan.RejectReason)

	res, err := h.o.ApplyManifest(ctx, ownerAlice(), manifest)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.Applied {
		t.Fatalf("batch must be rejected as a whole")
	}
	for i, it := range res.Items {
		if !cerr.Is(it.Err, cerr.KindQuota) {
			t.Fatalf("item %d err=%v want QuotaExceeded", i, it.Err)
		}
		t.Logf("OVER-QUOTA item %s -> %v", it.Name, it.Err)
	}

	// No side effects: no new envs, no queued ops, persisted doc identical,
	// existing env untouched (phase/expiry unchanged).
	if n := countEnvs(t, h.o); n != 1 {
		t.Fatalf("env count=%d want 1 (nothing created)", n)
	}
	if !bytes.Equal(before, stateFileBytes(t, h)) {
		t.Fatalf("rejected batch mutated persisted state")
	}
	got, _ := h.o.GetEnvironment(ctx, ownerAlice(), keep.ID)
	if got.Phase != model.PhaseRunning || got.Spec.Replicas != 5 {
		t.Fatalf("existing env disturbed: phase=%s replicas=%d", got.Phase, got.Spec.Replicas)
	}
	waitIdle(t, h.o)
	if st := h.backend.Stats(); st.Alive != 5 {
		t.Fatalf("backend alive=%d want 5 (no leaked allocations)", st.Alive)
	}
	t.Log("OVER-QUOTA: whole batch rejected, zero side effects verified")
}

// TestManifestItemErrorClassification covers scenario: one batch mixes a
// forbidden item (someone else's env), an invalid declaration and a valid
// create. The batch is rejected; each item's error kind distinguishes
// 越权 / 非法 / batch-reject, and the valid create is NOT applied.
func TestManifestItemErrorClassification(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	bobEnv := createEnv(t, h.o, ownerBob(), "bob-svc", 1)
	waitIdle(t, h.o)
	before := stateFileBytes(t, h)

	manifest := []ManifestItem{
		{Name: "ok-create", Image: "img", Replicas: 1, Resources: baseResources()},
		{Name: "bob-svc", Image: "img", Replicas: 2, Resources: baseResources()}, // alice touching bob's env
		{Name: "bad", Image: "img", Replicas: 0, Resources: baseResources()},     // invalid replicas
	}
	res, err := h.o.ApplyManifest(ctx, ownerAlice(), manifest)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.Applied {
		t.Fatalf("batch with inadmissible items must be rejected")
	}
	if !cerr.Is(res.Items[1].Err, cerr.KindForbidden) {
		t.Fatalf("item1 err=%v want Forbidden (cross-owner)", res.Items[1].Err)
	}
	if !cerr.Is(res.Items[2].Err, cerr.KindValidation) {
		t.Fatalf("item2 err=%v want ValidationError", res.Items[2].Err)
	}
	if res.Items[0].Err == nil {
		t.Fatalf("item0 must carry the batch-reject reason")
	}
	for _, it := range res.Items {
		t.Logf("CLASSIFY %s -> action=%s err=%v", it.Name, it.Action, it.Err)
	}

	// The valid create must not have happened (all-or-nothing).
	if n := countEnvs(t, h.o); n != 1 {
		t.Fatalf("env count=%d want 1: valid item leaked through a rejected batch", n)
	}
	if !bytes.Equal(before, stateFileBytes(t, h)) {
		t.Fatalf("rejected batch mutated persisted state")
	}
	got, _ := h.o.GetEnvironment(ctx, ownerBob(), bobEnv.ID)
	if got.Spec.Replicas != 1 {
		t.Fatalf("bob's env modified by rejected batch: replicas=%d", got.Spec.Replicas)
	}
	t.Log("CLASSIFY OK: forbidden/validation distinguished; no partial effects")
}

// TestManifestIdempotentReapply covers scenario: the same manifest applied
// twice. The second apply must plan as all-NoOp, enqueue nothing, write no
// state and cause no extra backend calls.
func TestManifestIdempotentReapply(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	manifest := []ManifestItem{
		{Name: "web", Image: "img:1", Replicas: 2, Resources: baseResources()},
		{Name: "worker", Image: "img:1", Replicas: 1, Resources: baseResources(), Desired: model.DesiredStopped},
	}
	res1, err := h.o.ApplyManifest(ctx, ownerAlice(), manifest)
	if err != nil || !res1.Applied {
		t.Fatalf("first apply: res=%+v err=%v", res1, err)
	}
	waitIdle(t, h.o)
	before := stateFileBytes(t, h)
	statsBefore := h.backend.Stats()

	res2, err := h.o.ApplyManifest(ctx, ownerAlice(), manifest)
	if err != nil || !res2.Applied {
		t.Fatalf("second apply: res=%+v err=%v", res2, err)
	}
	for i, it := range res2.Items {
		if it.Action != PlanNoOp {
			t.Fatalf("re-apply item %d action=%s want NoOp", i, it.Action)
		}
		t.Logf("IDEMPOTENT replay %s -> NoOp", it.Name)
	}
	waitIdle(t, h.o)
	if !bytes.Equal(before, stateFileBytes(t, h)) {
		t.Fatalf("idempotent re-apply rewrote persisted state")
	}
	statsAfter := h.backend.Stats()
	if statsAfter.CreateCalls != statsBefore.CreateCalls || statsAfter.DeleteCalls != statsBefore.DeleteCalls {
		t.Fatalf("re-apply caused backend calls: before=%+v after=%+v", statsBefore, statsAfter)
	}
	if n := countEnvs(t, h.o); n != 2 {
		t.Fatalf("env count=%d want 2 (no duplicates)", n)
	}
	t.Logf("IDEMPOTENT OK: replay produced no ops, no state writes, no backend calls (alive=%d)", statsAfter.Alive)
}

// TestManifestConcurrentDuplicateApply covers scenario: the same manifest is
// submitted concurrently many times. Admission is serialized; exactly one
// batch's worth of environments and allocations results.
func TestManifestConcurrentDuplicateApply(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	manifest := []ManifestItem{
		{Name: "a", Image: "img", Replicas: 2, Resources: baseResources()},
		{Name: "b", Image: "img", Replicas: 1, Resources: baseResources()},
	}
	const N = 8
	var wg sync.WaitGroup
	errs := make(chan error, N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := h.o.ApplyManifest(ctx, ownerAlice(), manifest)
			if err != nil {
				errs <- err
				return
			}
			if !res.Applied {
				errs <- cerr.New(cerr.KindConflict, "not applied: "+res.RejectReason)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent apply: %v", err)
	}
	waitIdle(t, h.o)

	if n := countEnvs(t, h.o); n != 2 {
		t.Fatalf("env count=%d want 2 after %d concurrent duplicate applies", n, N)
	}
	if st := h.backend.Stats(); st.Alive != 3 {
		t.Fatalf("alive=%d want 3 (no duplicate resources from concurrent applies)", st.Alive)
	}
	t.Logf("CONCURRENT OK: %d duplicate submits -> 2 envs, alive=%d", N, h.backend.Stats().Alive)
}

// TestManifestRestartConverges covers scenario: a manifest is applied and the
// process "restarts" before convergence finishes. After reopen, the same
// final state must be reached: created running, updated scaled, deleted gone.
func TestManifestRestartConverges(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()

	victim := createEnv(t, h.o, ownerAlice(), "victim", 1)
	scaler := createEnv(t, h.o, ownerAlice(), "scaler", 1)
	waitIdle(t, h.o)

	manifest := []ManifestItem{
		{Name: "fresh", Image: "img:1", Replicas: 2, Resources: baseResources()},
		{Name: "scaler", Image: "img:1", Replicas: 3, Resources: baseResources()},
		{Name: "victim", Desired: model.DesiredDeleted},
	}
	res, err := h.o.ApplyManifest(ctx, ownerAlice(), manifest)
	if err != nil || !res.Applied {
		t.Fatalf("apply: res=%+v err=%v", res, err)
	}
	// Restart immediately: convergence is mid-flight.
	h.o.Shutdown(ctx)

	var logs2 bytes.Buffer
	o2, b2 := reopenOrch(t, h, &logs2)
	defer o2.Shutdown(ctx)
	waitIdle(t, o2)

	if _, err := o2.GetEnvironment(ctx, ownerAlice(), victim.ID); !cerr.Is(err, cerr.KindNotFound) {
		t.Fatalf("victim should be deleted after restart, err=%v", err)
	}
	got, err := o2.GetEnvironment(ctx, ownerAlice(), scaler.ID)
	if err != nil {
		t.Fatalf("get scaler: %v", err)
	}
	if got.Phase != model.PhaseRunning || got.Spec.Replicas != 3 || len(got.Allocations) != 3 {
		t.Fatalf("scaler phase=%s replicas=%d allocs=%d want Running/3/3",
			got.Phase, got.Spec.Replicas, len(got.Allocations))
	}
	envs, _ := o2.ListEnvironments(ctx, ownerAlice())
	var fresh *model.Environment
	for _, e := range envs {
		if e.Spec.Name == "fresh" {
			fresh = e
		}
	}
	if fresh == nil || fresh.Phase != model.PhaseRunning || len(fresh.Allocations) != 2 {
		t.Fatalf("fresh env not converged after restart: %+v", fresh)
	}
	if st := b2.Stats(); st.Alive != 5 {
		t.Fatalf("alive=%d want 5 (3 scaler + 2 fresh, victim released, no leaks)", st.Alive)
	}
	t.Logf("RESTART OK: batch converged after reopen (fresh=2, scaler=3, victim gone, alive=%d)", b2.Stats().Alive)
}

// TestManifestIntraManifestConflict covers scenario: one manifest contains
// conflicting declarations for the same name. Resolution is deterministic
// last-wins, explainable per item, and cannot bypass quota or permissions.
func TestManifestIntraManifestConflict(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	manifest := []ManifestItem{
		{Name: "x", Image: "img", Replicas: 2, Resources: baseResources()}, // superseded
		{Name: "x", Image: "img", Replicas: 3, Resources: baseResources()}, // wins
		{Name: "y", Image: "img", Replicas: 1, Resources: baseResources()}, // superseded
		{Name: "y", Desired: model.DesiredDeleted},                         // wins: absent
	}
	plan, err := h.o.PreviewManifest(ctx, ownerAlice(), manifest)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if plan.Items[0].Action != PlanNoOp || plan.Items[2].Action != PlanNoOp {
		t.Fatalf("earlier duplicates must be NoOp, got %s / %s", plan.Items[0].Action, plan.Items[2].Action)
	}
	t.Logf("CONFLICT item0 reason=%q", plan.Items[0].Reason)
	// Net delta counts only the winning items: x=3 replicas, y absent.
	if plan.ClusterDelta.CPUMilli != 300 {
		t.Fatalf("cluster delta=%dm want 300m (only winning items count)", plan.ClusterDelta.CPUMilli)
	}

	res, err := h.o.ApplyManifest(ctx, ownerAlice(), manifest)
	if err != nil || !res.Applied {
		t.Fatalf("apply: res=%+v err=%v", res, err)
	}
	waitIdle(t, h.o)

	envs, _ := h.o.ListEnvironments(ctx, ownerAlice())
	if len(envs) != 1 || envs[0].Spec.Name != "x" || envs[0].Spec.Replicas != 3 {
		t.Fatalf("final state must be deterministic last-wins (x=3 only), got %+v", envs)
	}
	t.Logf("CONFLICT OK: last-wins -> x replicas=%d, y absent", envs[0].Spec.Replicas)
}

// TestManifestPartialConvergenceFailure covers scenario: the batch is
// admitted, but one item's materialization keeps failing at the backend.
// The healthy item converges; the failing one lands in Failed with an
// explainable reason — per-item final outcomes are distinguishable.
func TestManifestPartialConvergenceFailure(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	good := createEnv(t, h.o, ownerAlice(), "good", 1)
	waitIdle(t, h.o)

	// Fail every backend create that does not belong to the pre-existing
	// "good" env (allocation IDs are <envID>-g<gen>-<i>).
	h.backend.SetCreateHook(func(id string, attempt int) error {
		if strings.HasPrefix(id, good.ID+"-") {
			return nil
		}
		return fmt.Errorf("simulated infra outage for %s (attempt %d)", id, attempt)
	})

	res, err := h.o.ApplyManifest(ctx, ownerAlice(), []ManifestItem{
		{Name: "good", Image: "img:1", Replicas: 2, Resources: baseResources()}, // scale up: fine
		{Name: "doomed", Image: "img:1", Replicas: 1, Resources: baseResources()},
	})
	if err != nil || !res.Applied {
		t.Fatalf("apply: res=%+v err=%v", res, err)
	}
	waitIdle(t, h.o)

	g, _ := h.o.GetEnvironment(ctx, ownerAlice(), good.ID)
	if g.Phase != model.PhaseRunning || len(g.Allocations) != 2 {
		t.Fatalf("good env phase=%s allocs=%d want Running/2", g.Phase, len(g.Allocations))
	}
	envs, _ := h.o.ListEnvironments(ctx, ownerAlice())
	var doomed *model.Environment
	for _, e := range envs {
		if e.Spec.Name == "doomed" {
			doomed = e
		}
	}
	if doomed == nil {
		t.Fatalf("doomed env missing")
	}
	if doomed.Phase != model.PhaseFailed || doomed.FailReason == "" {
		t.Fatalf("doomed phase=%s failReason=%q want Failed with reason", doomed.Phase, doomed.FailReason)
	}
	t.Logf("PARTIAL outcome: good=Running(2) doomed=Failed reason=%q", doomed.FailReason)

	// Recovery path: fix the backend, re-apply the same manifest (retry),
	// and the failed item converges without touching the healthy one.
	h.backend.SetCreateHook(nil)
	res2, err := h.o.ApplyManifest(ctx, ownerAlice(), []ManifestItem{
		{Name: "good", Image: "img:1", Replicas: 2, Resources: baseResources()},
		{Name: "doomed", Image: "img:1", Replicas: 1, Resources: baseResources()},
	})
	if err != nil || !res2.Applied {
		t.Fatalf("re-apply: res=%+v err=%v", res2, err)
	}
	waitIdle(t, h.o)
	d, _ := h.o.GetEnvironment(ctx, ownerAlice(), doomed.ID)
	if d.Phase != model.PhaseRunning || len(d.Allocations) != 1 {
		t.Fatalf("doomed after retry phase=%s allocs=%d want Running/1", d.Phase, len(d.Allocations))
	}
	t.Log("PARTIAL recovery: re-apply retried the failed item -> Running")
}
