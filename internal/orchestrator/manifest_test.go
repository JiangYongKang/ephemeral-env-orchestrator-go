package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	cerr "github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// stateSnapshot captures everything a read-only preview must never touch.
type stateSnapshot struct {
	envsJSON  string
	opsCount  int
	batchCnt  int
	reserved  model.Resources
	usedAlive int
}

func snap(t *testing.T, h *testHolds) stateSnapshot {
	t.Helper()
	h.o.mu.Lock()
	defer h.o.mu.Unlock()
	envs, _ := json.Marshal(h.o.st.Envs)
	return stateSnapshot{
		envsJSON:  string(envs),
		opsCount:  len(h.o.st.Operations),
		batchCnt:  len(h.o.st.Batches),
		reserved:  model.TenantReserved(h.o.st, "t1"),
		usedAlive: h.backend.Stats().Alive,
	}
}

func (s stateSnapshot) assertUnchanged(t *testing.T, h *testHolds, what string) {
	t.Helper()
	now := snap(t, h)
	if now.envsJSON != s.envsJSON {
		t.Fatalf("%s mutated environments\nbefore: %s\nafter:  %s", what, firstNLines(s.envsJSON, 4), firstNLines(now.envsJSON, 4))
	}
	if now.opsCount != s.opsCount || now.batchCnt != s.batchCnt {
		t.Fatalf("%s mutated ops/batches: ops %d->%d batches %d->%d", what, s.opsCount, now.opsCount, s.batchCnt, now.batchCnt)
	}
	if now.reserved != s.reserved || now.usedAlive != s.usedAlive {
		t.Fatalf("%s mutated reservations/usage: reserved %v->%v alive %d->%d",
			what, s.reserved, now.reserved, s.usedAlive, now.usedAlive)
	}
	t.Logf("CHECK %s: state untouched (envs/ops/batches/reservations identical)", what)
}

func upsert(name string, replicas int) model.ManifestItem {
	return model.ManifestItem{
		Name: name, Image: "img:1", Replicas: replicas,
		Resources: baseResources(), TTLSeconds: 0,
	}
}

// TestManifestPreviewReadOnlyDeterministic covers: preview classifies
// create/update/delete/no-op with before/after diffs and net deltas, never
// mutates state, and is byte-stable across repetitions.
func TestManifestPreviewReadOnlyDeterministic(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	keep := createEnv(t, h.o, ownerAlice(), "keep", 2)   // 200 cpu reserved
	createEnv(t, h.o, ownerAlice(), "old", 1)            // 100 cpu reserved
	createEnv(t, h.o, ownerAlice(), "same", 1)           // 100 cpu reserved
	waitIdle(t, h.o)
	t.Log("SCENARIO preview: manifest with update+create+delete+noop against 3 running envs")

	mf := model.Manifest{Items: []model.ManifestItem{
		upsert("keep", 3), // update: 2->3 replicas, +100 cpu
		upsert("new-env", 2), // create: +200 cpu
		{Name: "old", Action: model.ManifestDelete}, // delete: -100 cpu
		upsert("same", 1), // no-op: identical declaration
	}}

	before := snap(t, h)
	pv1, err := h.o.PreviewManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	pv2, err := h.o.PreviewManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("preview again: %v", err)
	}
	j1, _ := json.Marshal(pv1)
	j2, _ := json.Marshal(pv2)
	if string(j1) != string(j2) {
		t.Fatalf("preview not deterministic:\n%s\nvs\n%s", j1, j2)
	}
	t.Log("CHECK repeated preview is byte-identical (deterministic)")

	kinds := map[string]model.ChangeKind{}
	for _, ip := range pv1.Items {
		kinds[ip.Name] = ip.Kind
		t.Logf("PREVIEW item#%d name=%s kind=%s delta=%+v diff=%v note=%q",
			ip.Index, ip.Name, ip.Kind, ip.Delta, ip.Diff, ip.Note)
	}
	want := map[string]model.ChangeKind{"keep": model.ChangeUpdate, "new-env": model.ChangeCreate, "old": model.ChangeDelete, "same": model.ChangeNoOp}
	for name, k := range want {
		if kinds[name] != k {
			t.Fatalf("item %s kind=%s want %s", name, kinds[name], k)
		}
	}
	if pv1.TenantDelta.CPUMilli != 200 || pv1.ClusterDelta.CPUMilli != 200 {
		t.Fatalf("net delta=%+v want +200 cpu (300+200-100)", pv1.TenantDelta)
	}
	if !pv1.Admissible {
		t.Fatalf("preview should be admissible, reason=%q", pv1.AdmissionReason)
	}
	t.Logf("DECISION net tenant/cluster delta=%+v admissible=%v (fits quota)", pv1.TenantDelta, pv1.Admissible)

	// The keep item must show the before/after spec diff.
	for _, ip := range pv1.Items {
		if ip.Name == "keep" {
			if ip.Before == nil || ip.After == nil || ip.Before.Replicas != 2 || ip.After.Replicas != 3 {
				t.Fatalf("keep before/after wrong: %+v -> %+v", ip.Before, ip.After)
			}
			if len(ip.Diff) == 0 || !strings.Contains(strings.Join(ip.Diff, ";"), "replicas: 2 -> 3") {
				t.Fatalf("keep diff missing replicas change: %v", ip.Diff)
			}
		}
	}
	t.Log("CHECK update item carries before/after specs and key-field diff")

	before.assertUnchanged(t, h, "preview")
	if got, _ := h.o.GetEnvironment(ctx, ownerAlice(), keep.ID); got.Spec.Replicas != 2 {
		t.Fatalf("preview changed spec: replicas=%d want 2", got.Spec.Replicas)
	}
}

// TestManifestNetSettlementReusesReleasedQuota covers: batch-level net
// settlement lets releases (delete/stop) inside the SAME manifest fund
// acquisitions (create), independent of execution order.
func TestManifestNetSettlementReusesReleasedQuota(t *testing.T) {
	var logs bytes.Buffer
	quotaVec := model.Resources{CPUMilli: 300, MemoryMB: 4096, StorageGB: 100}
	h := newTestOrch(t, &logs, quotaVec, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	victim := createEnv(t, h.o, ownerAlice(), "victim", 3) // 300 cpu: quota exactly full
	waitIdle(t, h.o)
	t.Log("SCENARIO net settlement: quota full (300/300); manifest deletes victim(300) AND creates fresh(300) in one batch")

	mf := model.Manifest{Items: []model.ManifestItem{
		upsert("fresh", 3), // +300 cpu
		{Name: "victim", Action: model.ManifestDelete}, // -300 cpu
	}}
	pv, err := h.o.PreviewManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if !pv.Admissible || pv.TenantDelta.CPUMilli != 0 {
		t.Fatalf("net-zero batch should be admissible: delta=%+v reason=%q", pv.TenantDelta, pv.AdmissionReason)
	}
	t.Logf("DECISION net delta=%+v -> admitted (delete funds create inside one batch)", pv.TenantDelta)

	batch, err := h.o.ApplyManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	waitIdle(t, h.o)

	if _, err := h.o.GetEnvironment(ctx, ownerAlice(), victim.ID); !cerr.Is(err, cerr.KindNotFound) {
		t.Fatalf("victim should be deleted, err=%v", err)
	}
	envs, _ := h.o.ListEnvironments(ctx, ownerAlice())
	if len(envs) != 1 || envs[0].Spec.Name != "fresh" || envs[0].Phase != model.PhaseRunning || len(envs[0].Allocations) != 3 {
		t.Fatalf("after apply envs=%v", envs)
	}
	done, _ := h.o.GetBatch(ctx, ownerAlice(), batch.ID)
	if !done.Done {
		t.Fatalf("batch not done: %+v", done.Items)
	}
	for _, bi := range done.Items {
		t.Logf("BATCH item#%d name=%s action=%s status=%s note=%q", bi.Index, bi.Name, bi.Action, bi.Status, bi.Note)
		if bi.Status != model.BatchItemApplied {
			t.Fatalf("item %s status=%s reason=%q", bi.Name, bi.Status, bi.Reason)
		}
	}
	t.Logf("STATE victim deleted, fresh Running 3/3, reserved still 300/300\n%s", firstNLines(logs.String(), 6))

	// Second scenario: stop (not delete) releases quota reusable in-batch.
	t.Log("SCENARIO net settlement: stop running env + create replacement in one batch")
	mf2 := model.Manifest{Items: []model.ManifestItem{
		{Name: "fresh", Action: model.ManifestUpsert, Image: "img:1", Replicas: 3,
			Resources: baseResources(), Desired: model.DesiredStopped}, // -300
		upsert("successor", 3), // +300
	}}
	if _, err := h.o.ApplyManifest(ctx, ownerAlice(), mf2); err != nil {
		t.Fatalf("apply stop+create: %v", err)
	}
	waitIdle(t, h.o)
	envs, _ = h.o.ListEnvironments(ctx, ownerAlice())
	phases := map[string]model.Phase{}
	for _, e := range envs {
		phases[e.Spec.Name] = e.Phase
	}
	if phases["fresh"] != model.PhaseStopped || phases["successor"] != model.PhaseRunning {
		t.Fatalf("phases=%v want fresh=Stopped successor=Running", phases)
	}
	t.Logf("STATE fresh=Stopped (quota released), successor=Running; reserved=%v", snap(t, h).reserved)
}

// TestManifestBatchRejectedNoSideEffects covers: net delta exceeding quota
// rejects the WHOLE manifest with per-item reasons and zero residue.
func TestManifestBatchRejectedNoSideEffects(t *testing.T) {
	var logs bytes.Buffer
	quotaVec := model.Resources{CPUMilli: 300, MemoryMB: 4096, StorageGB: 100}
	h := newTestOrch(t, &logs, quotaVec, bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	existing := createEnv(t, h.o, ownerAlice(), "existing", 3) // 300/300 full
	waitIdle(t, h.o)
	before := snap(t, h)
	expiresBefore := existing.ExpiresAt
	t.Log("SCENARIO rejection: quota full; manifest net delta +200 cpu -> whole batch refused")

	mf := model.Manifest{Items: []model.ManifestItem{
		upsert("too-big", 2), // +200 cpu over a full quota
		upsert("also-fine", 1), // valid on its own, but the batch stands or falls together
	}}
	_, err := h.o.ApplyManifest(ctx, ownerAlice(), mf)
	if err == nil {
		t.Fatal("apply should be rejected")
	}
	rej, ok := err.(*BatchRejection)
	if !ok {
		t.Fatalf("err type %T want *BatchRejection: %v", err, err)
	}
	if rej.Kind != cerr.KindQuota {
		t.Fatalf("rejection kind=%s want QuotaExceeded", rej.Kind)
	}
	for _, bi := range rej.Items {
		t.Logf("REJECT item#%d name=%s status=%s reason=%q", bi.Index, bi.Name, bi.Status, bi.Reason)
		if bi.Status != model.BatchItemFailed || !strings.Contains(bi.Reason, "quota") {
			t.Fatalf("item %s should fail with quota reason, got %q", bi.Name, bi.Reason)
		}
	}

	before.assertUnchanged(t, h, "rejected manifest")
	got, _ := h.o.GetEnvironment(ctx, ownerAlice(), existing.ID)
	if !got.ExpiresAt.Equal(expiresBefore) || got.Desired != model.DesiredRunning || got.Phase != model.PhaseRunning {
		t.Fatalf("rejected batch touched existing env: expires %v->%v desired=%s phase=%s",
			expiresBefore, got.ExpiresAt, got.Desired, got.Phase)
	}
	t.Log("CHECK rejected batch: no env created, no expiry moved, no desired flipped, no batch persisted")
}

// TestManifestBatchRejectedClassifiesErrors covers: per-item rejection
// reasons distinguish Forbidden / ValidationError / batch-level quota, and a
// batch containing ANY inadmissible item applies nothing at all.
func TestManifestBatchRejectedClassifiesErrors(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	bobs := createEnv(t, h.o, ownerBob(), "bobs-env", 1)
	waitIdle(t, h.o)
	before := snap(t, h)
	t.Log("SCENARIO rejection: alice's manifest mixes forbidden delete + invalid spec + one valid create")

	mf := model.Manifest{Items: []model.ManifestItem{
		{Name: "bobs-env", Action: model.ManifestDelete}, // Forbidden: not alice's
		{Name: "bad-spec", Image: "img:1", Replicas: 0, Resources: baseResources()}, // ValidationError
		upsert("would-be-fine", 1), // valid, but must NOT be applied
	}}
	_, err := h.o.ApplyManifest(ctx, ownerAlice(), mf)
	rej, ok := err.(*BatchRejection)
	if !ok {
		t.Fatalf("err type %T want *BatchRejection: %v", err, err)
	}
	reasons := map[string]string{}
	for _, bi := range rej.Items {
		reasons[bi.Name] = bi.Reason
		t.Logf("REJECT item#%d name=%s status=%s reason=%q", bi.Index, bi.Name, bi.Status, bi.Reason)
	}
	if !strings.Contains(reasons["bobs-env"], string(cerr.KindForbidden)) {
		t.Fatalf("bobs-env reason %q want Forbidden classification", reasons["bobs-env"])
	}
	if !strings.Contains(reasons["bad-spec"], string(cerr.KindValidation)) {
		t.Fatalf("bad-spec reason %q want ValidationError classification", reasons["bad-spec"])
	}
	if !strings.Contains(reasons["would-be-fine"], string(cerr.KindQuota)) && !strings.Contains(reasons["would-be-fine"], "batch") {
		t.Fatalf("would-be-fine reason %q want batch-level rejection", reasons["would-be-fine"])
	}

	before.assertUnchanged(t, h, "rejected mixed manifest")
	if _, err := h.o.GetEnvironment(ctx, ownerBob(), bobs.ID); err != nil {
		t.Fatalf("bob's env must survive alice's rejected manifest: %v", err)
	}
	envs, _ := h.o.ListEnvironments(ctx, ownerAlice())
	if len(envs) != 0 {
		t.Fatalf("valid item leaked through rejected batch: %v", envs)
	}
	t.Log("CHECK rejected batch: bob's env intact, valid item NOT applied, zero residue")
}

// TestManifestApplyIdempotentAndConcurrent covers: re-applying the same
// manifest is a no-op (same deterministic batch ID, no new ops, no backend
// calls), and concurrent duplicate submissions collapse into one batch.
func TestManifestApplyIdempotentAndConcurrent(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	mf := model.Manifest{Items: []model.ManifestItem{
		upsert("x", 2),
		upsert("y", 1),
	}}
	t.Log("SCENARIO idempotency: apply manifest, converge, apply the identical manifest again")

	b1, err := h.o.ApplyManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	waitIdle(t, h.o)
	statsAfterFirst := h.backend.Stats()
	before := snap(t, h)

	b2, err := h.o.ApplyManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if b2.ID != b1.ID {
		t.Fatalf("same manifest must map to same batch: %s vs %s", b2.ID, b1.ID)
	}
	for _, bi := range b2.Items {
		t.Logf("REAPPLY item#%d name=%s status=%s note=%q ops=%v", bi.Index, bi.Name, bi.Status, bi.Note, bi.OpIDs)
		if bi.Status != model.BatchItemApplied || len(bi.OpIDs) != 0 {
			t.Fatalf("re-apply item %s should be an op-free no-op, got status=%s ops=%v", bi.Name, bi.Status, bi.OpIDs)
		}
	}
	waitIdle(t, h.o)
	if st := h.backend.Stats(); st.CreateCalls != statsAfterFirst.CreateCalls || st.DeleteCalls != statsAfterFirst.DeleteCalls {
		t.Fatalf("re-apply caused backend calls: %+v -> %+v", statsAfterFirst, st)
	}
	before.assertUnchanged(t, h, "idempotent re-apply")
	t.Log("CHECK re-apply: same batch ID, zero ops, zero backend calls, state identical")

	// Concurrent duplicate submissions of the same manifest.
	t.Log("SCENARIO concurrency: 8 goroutines submit the identical manifest simultaneously")
	mf2 := model.Manifest{Items: []model.ManifestItem{
		upsert("shared", 2),
	}}
	const n = 8
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b, err := h.o.ApplyManifest(ctx, ownerAlice(), mf2)
			if b != nil {
				ids[i] = b.ID
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("concurrent apply %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("concurrent applies produced different batches: %s vs %s", ids[i], ids[0])
		}
	}
	waitIdle(t, h.o)
	envs, _ := h.o.ListEnvironments(ctx, ownerAlice())
	names := map[string]int{}
	for _, e := range envs {
		names[e.Spec.Name]++
	}
	if names["shared"] != 1 {
		t.Fatalf("concurrent duplicate apply created %d 'shared' envs, want 1", names["shared"])
	}
	t.Logf("CHECK concurrency: one batch %s, exactly one 'shared' env, alive=%d", ids[0], h.backend.Stats().Alive)
}

// TestManifestConflictingItemsDeterministic covers: same-name conflicts
// inside one manifest resolve deterministically (Delete wins, else last
// wins), are explained in preview/batch notes, and never bypass quota.
func TestManifestConflictingItemsDeterministic(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	victim := createEnv(t, h.o, ownerAlice(), "victim", 1)
	waitIdle(t, h.o)
	t.Log("SCENARIO conflict: upsert(2) + upsert(3) + delete on the same env in one manifest")

	mf := model.Manifest{Items: []model.ManifestItem{
		upsert("victim", 2),
		upsert("victim", 3),
		{Name: "victim", Action: model.ManifestDelete},
	}}
	pv, err := h.o.PreviewManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	for _, ip := range pv.Items {
		t.Logf("CONFLICT item#%d action-planned kind=%s note=%q", ip.Index, ip.Kind, ip.Note)
	}
	if pv.Items[0].Kind != model.ChangeNoOp || pv.Items[1].Kind != model.ChangeNoOp || pv.Items[2].Kind != model.ChangeDelete {
		t.Fatalf("conflict resolution wrong: %s %s %s", pv.Items[0].Kind, pv.Items[1].Kind, pv.Items[2].Kind)
	}
	if !strings.Contains(pv.Items[0].Note, "Delete wins") {
		t.Fatalf("superseded note must explain Delete wins: %q", pv.Items[0].Note)
	}
	if _, err := h.o.ApplyManifest(ctx, ownerAlice(), mf); err != nil {
		t.Fatalf("apply: %v", err)
	}
	waitIdle(t, h.o)
	if _, err := h.o.GetEnvironment(ctx, ownerAlice(), victim.ID); !cerr.Is(err, cerr.KindNotFound) {
		t.Fatalf("delete must win the conflict, err=%v", err)
	}
	t.Log("CHECK conflict: Delete beat both Upserts; env deleted deterministically")

	t.Log("SCENARIO conflict: two upserts, no delete -> last declaration wins")
	mf2 := model.Manifest{Items: []model.ManifestItem{
		upsert("contested", 2),
		upsert("contested", 3),
	}}
	pv2, _ := h.o.PreviewManifest(ctx, ownerAlice(), mf2)
	if pv2.Items[0].Kind != model.ChangeNoOp || !strings.Contains(pv2.Items[0].Note, "later declaration") {
		t.Fatalf("first upsert should be superseded by the later one: %+v", pv2.Items[0])
	}
	if _, err := h.o.ApplyManifest(ctx, ownerAlice(), mf2); err != nil {
		t.Fatalf("apply: %v", err)
	}
	waitIdle(t, h.o)
	envs, _ := h.o.ListEnvironments(ctx, ownerAlice())
	if len(envs) != 1 || envs[0].Spec.Replicas != 3 {
		t.Fatalf("last upsert must win: %+v", envs)
	}
	t.Log("CHECK conflict: later upsert won with replicas=3; result deterministic and explained")
}

// TestManifestRestartConverges covers: an apply interrupted mid-flight
// (process restart while creates are blocked) resumes and converges to the
// same result without duplicates or leaks.
func TestManifestRestartConverges(t *testing.T) {
	h := newTestOrch(t, &bytes.Buffer{}, smallQuota(), bigCapacity())
	ctx := context.Background()

	old := createEnv(t, h.o, ownerAlice(), "old", 2)
	waitIdle(t, h.o)
	t.Log("SCENARIO restart: manifest delete+create applied, then process 'dies' while backend creates are blocked")

	var blocked atomic.Bool
	blocked.Store(true)
	h.backend.SetCreateHook(func(id string, attempt int) error {
		if blocked.Load() {
			return errInfra("infra down before crash")
		}
		return nil
	})
	mf := model.Manifest{Items: []model.ManifestItem{
		{Name: "old", Action: model.ManifestDelete},
		upsert("new-a", 2),
		upsert("new-b", 1),
	}}
	batch, err := h.o.ApplyManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	timeSleep10() // a couple of blocked create attempts; delete progresses
	h.o.Shutdown(ctx)
	blocked.Store(false) // the "new process" sees a healed backend
	t.Logf("STATE simulated crash: batch=%s persisted with pending items", batch.ID)

	var logs2 bytes.Buffer
	o2, b2 := reopenOrch(t, h, &logs2)
	defer o2.Shutdown(ctx)
	waitIdle(t, o2)

	if _, err := o2.GetEnvironment(ctx, ownerAlice(), old.ID); !cerr.Is(err, cerr.KindNotFound) {
		t.Fatalf("old env must stay deleted across restart, err=%v", err)
	}
	envs, _ := o2.ListEnvironments(ctx, ownerAlice())
	got := map[string]int{}
	for _, e := range envs {
		if e.Phase != model.PhaseRunning {
			t.Fatalf("env %s phase=%s want Running after recovery", e.Spec.Name, e.Phase)
		}
		got[e.Spec.Name] = len(e.Allocations)
	}
	if got["new-a"] != 2 || got["new-b"] != 1 || len(envs) != 2 {
		t.Fatalf("post-restart envs=%v want new-a=2 new-b=1", got)
	}
	done, err := o2.GetBatch(ctx, ownerAlice(), batch.ID)
	if err != nil {
		t.Fatalf("batch must survive restart: %v", err)
	}
	if !done.Done {
		t.Fatalf("batch not converged after restart: %+v", done.Items)
	}
	for _, bi := range done.Items {
		t.Logf("RECOVERED batch item#%d name=%s status=%s note=%q", bi.Index, bi.Name, bi.Status, bi.Note)
		if bi.Status != model.BatchItemApplied {
			t.Fatalf("item %s status=%s reason=%q after restart", bi.Name, bi.Status, bi.Reason)
		}
	}
	if st := b2.Stats(); st.Alive != 3 {
		t.Fatalf("alive=%d want 3 (duplicate or leak across restart)", st.Alive)
	}
	t.Logf("CHECK restart: batch converged, no duplicate/leak (alive=3)\n%s", firstNLines(logs2.String(), 8))
}

// TestManifestPartialFailureExplained covers: when one item ultimately fails
// (backend hard failure) the batch record explains per item what succeeded,
// what failed and why; the batch is also visible only to its actor/admin.
func TestManifestPartialFailureExplained(t *testing.T) {
	var logs bytes.Buffer
	h := newTestOrch(t, &logs, smallQuota(), bigCapacity())
	ctx := context.Background()
	defer h.o.Shutdown(ctx)

	// Fail every create except explicitly whitelisted allocation IDs; the
	// whitelist is filled synchronously right after ApplyManifest returns,
	// long before the retry budget (5 attempts) runs out.
	var allow sync.Map
	h.backend.SetCreateHook(func(id string, attempt int) error {
		if _, ok := allow.Load(id); !ok {
			return errInfra("infra permanently broken for " + id)
		}
		return nil
	})
	t.Log("SCENARIO partial failure: two creates; backend always fails bad's allocation")

	mf := model.Manifest{Items: []model.ManifestItem{
		upsert("good", 1),
		upsert("bad", 1),
	}}
	batch, err := h.o.ApplyManifest(ctx, ownerAlice(), mf)
	if err != nil {
		t.Fatalf("apply admitted (failure happens at convergence): %v", err)
	}
	for _, bi := range batch.Items {
		if bi.Name == "good" {
			allow.Store(bi.EnvID+"-g1-0", true)
		}
	}
	waitIdle(t, h.o)

	done, err := h.o.GetBatch(ctx, ownerAlice(), batch.ID)
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	if !done.Done {
		t.Fatalf("batch should reach terminal state: %+v", done.Items)
	}
	status := map[string]*model.BatchItem{}
	for _, bi := range done.Items {
		status[bi.Name] = bi
		t.Logf("OUTCOME item#%d name=%s status=%s reason=%q", bi.Index, bi.Name, bi.Status, bi.Reason)
	}
	if status["good"].Status != model.BatchItemApplied {
		t.Fatalf("good item=%s want Applied", status["good"].Status)
	}
	if status["bad"].Status != model.BatchItemFailed || !strings.Contains(status["bad"].Reason, "infra permanently broken") {
		t.Fatalf("bad item=%s reason=%q want Failed with backend cause", status["bad"].Status, status["bad"].Reason)
	}
	envs, _ := h.o.ListEnvironments(ctx, ownerAlice())
	phases := map[string]model.Phase{}
	for _, e := range envs {
		phases[e.Spec.Name] = e.Phase
	}
	if phases["good"] != model.PhaseRunning || phases["bad"] != model.PhaseFailed {
		t.Fatalf("phases=%v want good=Running bad=Failed", phases)
	}
	t.Logf("STATE good=Running, bad=Failed (failReason surfaced in batch item)")

	// Batch visibility: only the actor (or admin) can read it.
	if _, err := h.o.GetBatch(ctx, ownerBob(), batch.ID); !cerr.Is(err, cerr.KindNotFound) {
		t.Fatalf("bob must not see alice's batch, err=%v", err)
	}
	if _, err := h.o.GetBatch(ctx, admin(), batch.ID); err != nil {
		t.Fatalf("admin must see the batch: %v", err)
	}
	t.Log("CHECK batch visibility: owner+admin only (others get NotFound)")
}
