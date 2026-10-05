package orchestrator

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// ManifestItem declares the desired state of one environment within a
// manifest. An empty Desired defaults to Running; Desired=Deleted declares
// that the environment must be absent (spec fields are then ignored).
type ManifestItem struct {
	Name       string
	Image      string
	Replicas   int
	Resources  model.Resources
	TTLSeconds int
	Desired    model.Desired
}

// PlanAction classifies what one manifest item would do.
type PlanAction string

const (
	PlanCreate PlanAction = "Create"
	PlanUpdate PlanAction = "Update"
	PlanDelete PlanAction = "Delete"
	PlanNoOp   PlanAction = "NoOp"
)

// FieldDiff records one changed declared field (before -> after).
type FieldDiff struct {
	Field string
	From  string
	To    string
}

// PlanItem is the per-item preview outcome: the determined action, the key
// declaration differences, the explainable reason, and a classified error
// when the item itself is not admissible.
type PlanItem struct {
	Name   string
	EnvID  string
	Action PlanAction
	Diff   []FieldDiff
	Reason string
	Err    error
}

// Plan is the read-only preview of a whole manifest. TenantDelta/ClusterDelta
// are the net reserved-resource changes the manifest would cause; negative
// dimensions mean the batch releases resources.
type Plan struct {
	Items        []PlanItem
	TenantDelta  map[string]model.Resources
	ClusterDelta model.Resources
	Applicable   bool
	RejectReason string
	// QuotaExceeded records that (part of) the rejection was caused by
	// tenant quota or cluster capacity, so a rejected apply can classify
	// otherwise-clean items as QuotaExceeded rather than a generic conflict.
	QuotaExceeded bool
}

// ApplyItemResult reports the admission outcome of one item.
type ApplyItemResult struct {
	Name   string
	EnvID  string
	Action PlanAction
	Err    error
}

// ApplyResult reports whether the batch was admitted and per-item outcomes.
type ApplyResult struct {
	Applied      bool
	RejectReason string
	Items        []ApplyItemResult
}

// normalizeItem applies defaults and reports whether the declared desired
// state is a known value.
func normalizeItem(it ManifestItem) ManifestItem {
	if it.Desired == "" {
		it.Desired = model.DesiredRunning
	}
	return it
}

func validDesired(d model.Desired) bool {
	return d == model.DesiredRunning || d == model.DesiredStopped || d == model.DesiredDeleted
}

// reservedOfEnv mirrors the quota reservation model: an environment reserves
// its declared spec only while Desired=Running.
func reservedOfEnv(e *model.Environment) model.Resources {
	if e.Desired == model.DesiredRunning {
		return e.Spec.Requested()
	}
	return model.Resources{}
}

// reservedOfItem is the prospective reservation of a declared item.
func reservedOfItem(it ManifestItem) model.Resources {
	if it.Desired == model.DesiredRunning {
		return it.Resources.Mul(it.Replicas)
	}
	return model.Resources{}
}

func fmtRes(r model.Resources) string {
	return fmt.Sprintf("{cpu:%dm mem:%dMB storage:%dGB}", r.CPUMilli, r.MemoryMB, r.StorageGB)
}

// diffSpec lists the key declaration differences between the current env and
// the declared item.
func diffSpec(env *model.Environment, it ManifestItem) []FieldDiff {
	var d []FieldDiff
	if env.Spec.Image != it.Image {
		d = append(d, FieldDiff{Field: "image", From: env.Spec.Image, To: it.Image})
	}
	if env.Spec.Replicas != it.Replicas {
		d = append(d, FieldDiff{Field: "replicas",
			From: fmt.Sprintf("%d", env.Spec.Replicas), To: fmt.Sprintf("%d", it.Replicas)})
	}
	if env.Spec.Resources != it.Resources {
		d = append(d, FieldDiff{Field: "resources",
			From: fmtRes(env.Spec.Resources), To: fmtRes(it.Resources)})
	}
	if env.Spec.TTLSeconds != it.TTLSeconds {
		d = append(d, FieldDiff{Field: "ttlSeconds",
			From: fmt.Sprintf("%d", env.Spec.TTLSeconds), To: fmt.Sprintf("%d", it.TTLSeconds)})
	}
	if env.Desired != it.Desired {
		d = append(d, FieldDiff{Field: "desired",
			From: string(env.Desired), To: string(it.Desired)})
	}
	return d
}

// planItemLocked classifies one normalized manifest item against current
// state. It is strictly read-only. Returns the plan item plus the old/new
// reserved vectors used for batch net-delta settlement.
func (o *Orchestrator) planItemLocked(actor Actor, it ManifestItem) (PlanItem, model.Resources, model.Resources) {
	pi := PlanItem{Name: it.Name}
	zero := model.Resources{}

	if !validDesired(it.Desired) {
		pi.Err = errors.New(errors.KindValidation, "desired must be Running, Stopped or Deleted, got "+string(it.Desired))
		return pi, zero, zero
	}

	// Delete declaration: only the name matters.
	if it.Desired == model.DesiredDeleted {
		if it.Name == "" {
			pi.Err = errors.New(errors.KindValidation, "environment name is required")
			return pi, zero, zero
		}
		env := o.findByNameLocked(actor.TenantID, it.Name)
		if env == nil {
			pi.Action = PlanNoOp
			pi.Reason = "already absent: nothing to delete"
			return pi, zero, zero
		}
		pi.EnvID = env.ID
		if err := o.authorize(actor, env); err != nil {
			pi.Err = err
			return pi, zero, zero
		}
		old := reservedOfEnv(env)
		if env.Desired == model.DesiredDeleted {
			pi.Action = PlanNoOp
			pi.Reason = "deletion already in progress"
			return pi, old, old // reservation already released by the in-flight delete
		}
		pi.Action = PlanDelete
		pi.Reason = "declared absent; releases its reservation"
		pi.Diff = []FieldDiff{{Field: "desired", From: string(env.Desired), To: string(model.DesiredDeleted)}}
		return pi, old, zero
	}

	// Create/update declaration: full spec validation.
	if err := validateSpec(it.Name, it.Image, it.Replicas, it.Resources, it.TTLSeconds); err != nil {
		pi.Err = err
		return pi, zero, zero
	}
	newRes := reservedOfItem(it)

	env := o.findByNameLocked(actor.TenantID, it.Name)
	if env == nil {
		pi.Action = PlanCreate
		pi.Reason = "no active environment with this name in the tenant"
		pi.Diff = []FieldDiff{
			{Field: "image", From: "-", To: it.Image},
			{Field: "replicas", From: "-", To: fmt.Sprintf("%d", it.Replicas)},
			{Field: "resources", From: "-", To: fmtRes(it.Resources)},
			{Field: "desired", From: "-", To: string(it.Desired)},
		}
		return pi, zero, newRes
	}

	pi.EnvID = env.ID
	if err := o.authorize(actor, env); err != nil {
		pi.Err = err
		return pi, zero, zero
	}
	if env.Desired == model.DesiredDeleted {
		pi.Err = errors.New(errors.KindConflict,
			"environment "+env.ID+" is being deleted; redeclare after the deletion completes")
		return pi, zero, zero
	}

	oldRes := reservedOfEnv(env)
	diff := diffSpec(env, it)
	var notes []string
	renew := env.Phase == model.PhaseExpired && it.Desired == model.DesiredRunning
	if renew {
		notes = append(notes, "renews the expired environment (expiry re-anchored at apply time)")
	}
	retry := env.Phase == model.PhaseFailed && it.Desired == model.DesiredRunning
	if retry {
		notes = append(notes, "retries the failed environment")
	}
	if len(diff) == 0 && !renew && !retry {
		pi.Action = PlanNoOp
		pi.Reason = "declaration already satisfied"
		return pi, oldRes, oldRes
	}
	pi.Action = PlanUpdate
	pi.Diff = diff
	if len(diff) > 0 {
		fields := make([]string, 0, len(diff))
		for _, d := range diff {
			fields = append(fields, d.Field)
		}
		notes = append([]string{"declared fields differ: " + strings.Join(fields, ", ")}, notes...)
	}
	pi.Reason = strings.Join(notes, "; ")
	return pi, oldRes, newRes
}

// planLocked computes the read-only preview for a manifest. Deterministic:
// same state + same manifest always yields the same plan. Duplicate names
// within one manifest resolve last-wins (earlier items are marked superseded).
func (o *Orchestrator) planLocked(actor Actor, items []ManifestItem) *Plan {
	plan := &Plan{
		Items:       make([]PlanItem, len(items)),
		TenantDelta: map[string]model.Resources{},
	}

	// 1) Deterministic intra-manifest conflict resolution: last wins.
	lastIdx := map[string]int{}
	for i, it := range items {
		lastIdx[it.Name] = i
	}
	type effItem struct {
		idx int
		it  ManifestItem
	}
	var eff []effItem
	for i, it := range items {
		if lastIdx[it.Name] != i {
			plan.Items[i] = PlanItem{
				Name:   it.Name,
				Action: PlanNoOp,
				Reason: "superseded by a later item for the same name (deterministic last-wins)",
			}
			continue
		}
		eff = append(eff, effItem{idx: i, it: normalizeItem(it)})
	}

	// 2) Classify each effective item and accumulate net reserved deltas.
	var reject []string
	itemErrors := 0
	for _, e := range eff {
		pi, oldRes, newRes := o.planItemLocked(actor, e.it)
		plan.Items[e.idx] = pi
		if pi.Err != nil {
			itemErrors++
			continue
		}
		tenant := actor.TenantID
		delta := plan.TenantDelta[tenant]
		d := model.Resources{
			CPUMilli:  newRes.CPUMilli - oldRes.CPUMilli,
			MemoryMB:  newRes.MemoryMB - oldRes.MemoryMB,
			StorageGB: newRes.StorageGB - oldRes.StorageGB,
		}
		plan.TenantDelta[tenant] = model.Resources{
			CPUMilli:  delta.CPUMilli + d.CPUMilli,
			MemoryMB:  delta.MemoryMB + d.MemoryMB,
			StorageGB: delta.StorageGB + d.StorageGB,
		}
		plan.ClusterDelta = plan.ClusterDelta.Add(d)
	}
	if itemErrors > 0 {
		reject = append(reject, fmt.Sprintf("%d item(s) are not admissible (see per-item errors)", itemErrors))
	}

	// 3) Batch net-change settlement: the whole batch's net delta must fit on
	// top of current reservations, per tenant and cluster-wide. Releases from
	// deletes/stops in the same batch therefore fund creates/growth.
	quotaRejected := false
	tenants := make([]string, 0, len(plan.TenantDelta))
	for tenant := range plan.TenantDelta {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	for _, tenant := range tenants {
		delta := plan.TenantDelta[tenant]
		projected := model.TenantReserved(o.st, tenant).Add(delta)
		q, ok := o.st.Quotas[tenant]
		if !ok {
			if projected.CPUMilli > 0 || projected.MemoryMB > 0 || projected.StorageGB > 0 {
				quotaRejected = true
				reject = append(reject, fmt.Sprintf("tenant %q has no quota configured", tenant))
			}
			continue
		}
		if projected.CPUMilli > q.CPUMilli {
			quotaRejected = true
			reject = append(reject, fmt.Sprintf(
				"tenant %q cpu quota exceeded by batch net change: reserved=%d net=%+d > limit=%d milli",
				tenant, model.TenantReserved(o.st, tenant).CPUMilli, delta.CPUMilli, q.CPUMilli))
		}
		if projected.MemoryMB > q.MemoryMB {
			quotaRejected = true
			reject = append(reject, fmt.Sprintf(
				"tenant %q memory quota exceeded by batch net change: reserved=%d net=%+d > limit=%d MB",
				tenant, model.TenantReserved(o.st, tenant).MemoryMB, delta.MemoryMB, q.MemoryMB))
		}
		if projected.StorageGB > q.StorageGB {
			quotaRejected = true
			reject = append(reject, fmt.Sprintf(
				"tenant %q storage quota exceeded by batch net change: reserved=%d net=%+d > limit=%d GB",
				tenant, model.TenantReserved(o.st, tenant).StorageGB, delta.StorageGB, q.StorageGB))
		}
	}
	clusterProjected := model.ClusterReserved(o.st).Add(plan.ClusterDelta)
	cap := o.st.Capacity
	if clusterProjected.CPUMilli > cap.CPUMilli {
		quotaRejected = true
		reject = append(reject, fmt.Sprintf(
			"cluster cpu capacity exceeded by batch net change: reserved=%d net=%+d > capacity=%d milli",
			model.ClusterReserved(o.st).CPUMilli, plan.ClusterDelta.CPUMilli, cap.CPUMilli))
	}
	if clusterProjected.MemoryMB > cap.MemoryMB {
		quotaRejected = true
		reject = append(reject, fmt.Sprintf(
			"cluster memory capacity exceeded by batch net change: reserved=%d net=%+d > capacity=%d MB",
			model.ClusterReserved(o.st).MemoryMB, plan.ClusterDelta.MemoryMB, cap.MemoryMB))
	}
	if clusterProjected.StorageGB > cap.StorageGB {
		quotaRejected = true
		reject = append(reject, fmt.Sprintf(
			"cluster storage capacity exceeded by batch net change: reserved=%d net=%+d > capacity=%d GB",
			model.ClusterReserved(o.st).StorageGB, plan.ClusterDelta.StorageGB, cap.StorageGB))
	}

	plan.Applicable = len(reject) == 0
	plan.RejectReason = strings.Join(reject, "; ")
	plan.QuotaExceeded = quotaRejected
	return plan
}

// PreviewManifest computes a read-only change preview for a manifest: per-item
// action (Create/Update/Delete/NoOp) with key declaration diffs and reasons,
// plus the net reserved-resource delta per tenant and cluster-wide. It never
// mutates environments, operations, quotas or any persisted state.
func (o *Orchestrator) PreviewManifest(ctx context.Context, actor Actor, items []ManifestItem) (*Plan, error) {
	if actor.Name == "" || actor.TenantID == "" {
		return nil, errors.New(errors.KindUnauthorized, "unauthenticated actor")
	}
	if actor.Role != RoleOwner && actor.Role != RoleAdmin {
		return nil, errors.New(errors.KindUnauthorized, "unknown actor role")
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	plan := o.planLocked(actor, items)
	actions := map[PlanAction]int{}
	for _, pi := range plan.Items {
		actions[pi.Action]++
	}
	o.log.Info("manifest previewed (read-only)",
		"actor", actor.Name, "tenant", actor.TenantID, "items", len(items),
		"creates", actions[PlanCreate], "updates", actions[PlanUpdate],
		"deletes", actions[PlanDelete], "noops", actions[PlanNoOp],
		"clusterDelta", fmtRes(plan.ClusterDelta),
		"applicable", plan.Applicable, "rejectReason", plan.RejectReason)
	return plan, nil
}

// ApplyManifest admits a whole manifest atomically and lets the
// reconciliation loop converge it.
//
// Admission is all-or-nothing: the manifest is planned under the state lock;
// if any item is inadmissible (unauthorized, invalid declaration, conflicting
// in-flight deletion) or the batch net change exceeds tenant quota or cluster
// capacity, nothing is mutated or persisted and every item carries a
// classified reason. When admitted, all declarations (env records, desired
// states, queued ops) are persisted in a single atomic save, so a crash
// either loses the whole batch or converges it — never a partial batch.
//
// Re-applying the same manifest is idempotent: satisfied items plan as NoOp
// and identical pending ops are de-duplicated at enqueue time.
func (o *Orchestrator) ApplyManifest(ctx context.Context, actor Actor, items []ManifestItem) (*ApplyResult, error) {
	if actor.Name == "" || actor.TenantID == "" {
		return nil, errors.New(errors.KindUnauthorized, "unauthenticated actor")
	}
	if actor.Role != RoleOwner && actor.Role != RoleAdmin {
		return nil, errors.New(errors.KindUnauthorized, "unknown actor role")
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	plan := o.planLocked(actor, items)
	res := &ApplyResult{Items: make([]ApplyItemResult, len(items))}
	for i, pi := range plan.Items {
		res.Items[i] = ApplyItemResult{Name: pi.Name, EnvID: pi.EnvID, Action: pi.Action, Err: pi.Err}
	}

	if !plan.Applicable {
		res.Applied = false
		res.RejectReason = plan.RejectReason
		// Items without their own error are rejected only because the batch
		// failed; classify them with the batch cause.
		for i := range res.Items {
			if res.Items[i].Err != nil {
				continue
			}
			if plan.QuotaExceeded {
				res.Items[i].Err = errors.New(errors.KindQuota,
					"batch rejected: "+plan.RejectReason)
			} else {
				res.Items[i].Err = errors.New(errors.KindConflict,
					"batch rejected: "+plan.RejectReason)
			}
		}
		o.log.Warn("manifest apply rejected as a whole (no side effects)",
			"actor", actor.Name, "tenant", actor.TenantID, "items", len(items),
			"quotaExceeded", plan.QuotaExceeded, "reason", plan.RejectReason)
		return res, nil
	}

	// Admitted: mutate every effective item under the same lock, then persist
	// once. The plan was computed from this very lock scope, so applying it
	// cannot diverge from the preview.
	lastIdx := map[string]int{}
	for i, it := range items {
		lastIdx[it.Name] = i
	}
	mutated := false
	for i, raw := range items {
		if lastIdx[raw.Name] != i {
			continue // superseded duplicate: nothing to do
		}
		it := normalizeItem(raw)
		pi := plan.Items[i]
		switch pi.Action {
		case PlanNoOp:
			o.log.Info("manifest item no-op", "name", it.Name, "reason", pi.Reason)
		case PlanCreate:
			res.Items[i].EnvID = o.applyCreateItemLocked(actor, it)
			mutated = true
		case PlanUpdate:
			o.applyUpdateItemLocked(actor, it, pi.EnvID)
			mutated = true
		case PlanDelete:
			o.applyDeleteItemLocked(actor, pi.EnvID)
			mutated = true
		}
	}
	// A fully-satisfied replay mutates nothing and must not even rewrite the
	// persisted document (true idempotency: no extra actions at all).
	if mutated {
		if err := o.saveLocked(); err != nil {
			return nil, errors.New(errors.KindInternal, err.Error())
		}
	}
	res.Applied = true
	o.log.Info("manifest applied (declarations persisted; convergence in progress)",
		"actor", actor.Name, "tenant", actor.TenantID, "items", len(items),
		"clusterDelta", fmtRes(plan.ClusterDelta))
	return res, nil
}

// applyCreateItemLocked records a new environment declaration. Quota was
// already settled as a batch net change, so no per-item re-check happens
// here. Returns the new env ID.
func (o *Orchestrator) applyCreateItemLocked(actor Actor, it ManifestItem) string {
	now := o.now()
	envID := "env-" + strconv.FormatInt(o.nextSeqLocked(), 10)
	spec := model.EnvSpec{
		Name:       it.Name,
		Owner:      actor.Name,
		TenantID:   actor.TenantID,
		Image:      it.Image,
		Replicas:   it.Replicas,
		Resources:  it.Resources,
		TTLSeconds: it.TTLSeconds,
		CreatedAt:  now,
	}
	expiry := time.Time{}
	if it.TTLSeconds > 0 {
		expiry = now.Add(time.Duration(it.TTLSeconds) * time.Second)
	}
	env := &model.Environment{
		ID:         envID,
		Spec:       spec,
		Desired:    it.Desired,
		Phase:      model.PhasePending,
		Generation: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  expiry,
	}
	o.st.Envs[envID] = env
	if it.Desired == model.DesiredRunning {
		op := o.enqueueLocked(model.OpCreate, envID, actor, nil)
		o.log.Info("manifest create declared", "envId", envID, "name", it.Name, "op", op.ID)
	} else {
		// Declared Stopped: the record exists but reserves and materializes
		// nothing until a later declaration asks for Running.
		env.Phase = model.PhaseStopped
		o.log.Info("manifest create declared (stopped)", "envId", envID, "name", it.Name)
	}
	return envID
}

// applyUpdateItemLocked merges the declared item into an existing env:
// spec changes queue an Update op, desired transitions queue Start/Stop ops,
// expiry is re-anchored on TTL change or renewal, and a Failed env declared
// Running gets a retry. Identical queued ops are de-duplicated by enqueue.
func (o *Orchestrator) applyUpdateItemLocked(actor Actor, it ManifestItem, envID string) {
	env := o.st.Envs[envID]
	if env == nil {
		return // cannot happen: plan and apply share one lock scope
	}
	now := o.now()

	candidate := env.Spec
	candidate.Image = it.Image
	candidate.Replicas = it.Replicas
	candidate.Resources = it.Resources
	candidate.TTLSeconds = it.TTLSeconds
	specChanged := candidate.Image != env.Spec.Image ||
		candidate.Resources != env.Spec.Resources ||
		candidate.Replicas != env.Spec.Replicas ||
		candidate.TTLSeconds != env.Spec.TTLSeconds

	renew := env.Phase == model.PhaseExpired && it.Desired == model.DesiredRunning
	if candidate.TTLSeconds != env.Spec.TTLSeconds || renew {
		// Re-anchor expiry from now (TTL change or renewal of an expired env).
		if candidate.TTLSeconds > 0 {
			env.ExpiresAt = now.Add(time.Duration(candidate.TTLSeconds) * time.Second)
		} else {
			env.ExpiresAt = time.Time{}
		}
	}
	if renew {
		env.Phase = model.PhasePending
		env.FailReason = ""
	}

	if specChanged {
		specCopy := candidate
		op := o.enqueueLocked(model.OpUpdate, envID, actor, &specCopy)
		o.log.Info("manifest update queued", "envId", envID, "op", op.ID)
	}

	switch it.Desired {
	case model.DesiredStopped:
		if env.Desired != model.DesiredStopped {
			env.Desired = model.DesiredStopped
			op := o.enqueueLocked(model.OpStop, envID, actor, nil)
			o.log.Info("manifest stop queued", "envId", envID, "op", op.ID)
		}
	case model.DesiredRunning:
		if env.Desired != model.DesiredRunning {
			env.Desired = model.DesiredRunning
			op := o.enqueueLocked(model.OpStart, envID, actor, nil)
			o.log.Info("manifest start queued", "envId", envID, "op", op.ID)
		} else if env.Phase == model.PhaseFailed {
			// Re-declaring Running on a Failed env is an explicit retry.
			op := o.enqueueLocked(model.OpStart, envID, actor, nil)
			o.log.Info("manifest retry queued for failed env", "envId", envID, "op", op.ID)
		}
	}
	env.UpdatedAt = now
}

// applyDeleteItemLocked declares deletion; the reconcile loop releases all
// allocations and the reservation, resumably.
func (o *Orchestrator) applyDeleteItemLocked(actor Actor, envID string) {
	env := o.st.Envs[envID]
	if env == nil {
		return
	}
	env.Desired = model.DesiredDeleted
	env.UpdatedAt = o.now()
	op := o.enqueueLocked(model.OpDelete, envID, actor, nil)
	o.log.Info("manifest delete queued", "envId", envID, "op", op.ID)
}
