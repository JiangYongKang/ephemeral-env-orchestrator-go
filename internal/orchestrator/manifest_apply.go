package orchestrator

import (
	"context"
	"strconv"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// BatchRejection is returned when a manifest is refused as a whole. It
// carries the per-item classified reasons (unauthorized / invalid /
// insufficient resources) and guarantees: not a single environment, operation
// or reservation was touched — a rejected batch leaves zero residue.
type BatchRejection struct {
	Kind   errors.Kind
	Reason string
	Items  []model.BatchItem
}

func (e *BatchRejection) Error() string {
	return string(e.Kind) + ": manifest rejected as a whole: " + e.Reason
}

// batchItem builds the persisted tracking record for one planned item.
func batchItemOf(pi *plannedItem, opIDs []string) *model.BatchItem {
	bi := &model.BatchItem{
		Index:  pi.index,
		Name:   pi.item.Name,
		Action: pi.item.Action,
		Note:   pi.note,
		OpIDs:  opIDs,
	}
	if pi.env != nil {
		bi.EnvID = pi.env.ID
	}
	if pi.item.Action == model.ManifestUpsert && !pi.superseded {
		spec := pi.after
		bi.Spec = &spec
		bi.Desired = pi.desired
	}
	switch {
	case pi.superseded:
		bi.Status = model.BatchItemApplied
	case pi.kind == model.ChangeNoOp:
		bi.Status = model.BatchItemApplied
	default:
		bi.Status = model.BatchItemPending
	}
	return bi
}

// rejectItem builds a failed item record explaining why it was refused.
func rejectItem(pi *plannedItem, batchReason string) model.BatchItem {
	bi := model.BatchItem{
		Index:  pi.index,
		Name:   pi.item.Name,
		Action: pi.item.Action,
		Status: model.BatchItemFailed,
		Note:   pi.note,
	}
	if pi.env != nil {
		bi.EnvID = pi.env.ID
	}
	switch {
	case pi.err != nil:
		bi.Reason = pi.err.Error() // classified: ValidationError/Forbidden/...
	case pi.superseded || pi.kind == model.ChangeNoOp:
		bi.Status = model.BatchItemApplied // losers/no-ops are not "rejected"
		bi.Reason = ""
	default:
		bi.Reason = "rejected with the batch: " + batchReason
	}
	return bi
}

// ApplyManifest validates, settles and enqueues a whole manifest atomically.
//
// Admission is all-or-nothing: any per-item validation/auth error, or a net
// reservation delta that exceeds tenant quota or cluster capacity, rejects
// the ENTIRE manifest with per-item reasons and zero side effects.
//
// The batch is settled on the NET delta, so releases from deletes/stops in
// the same manifest fund creates/expansions in it. Execution order can never
// change the admission decision.
//
// Re-applying the same manifest (same deterministic ID) is idempotent:
// already-satisfied items plan as NoOp, identical intents dedupe against
// pending operations, and the persisted batch record is reused — concurrent
// duplicate submissions converge to one batch with no duplicate work.
func (o *Orchestrator) ApplyManifest(ctx context.Context, actor Actor, mf model.Manifest) (*model.Batch, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	p, err := o.planLocked(actor, mf)
	if err != nil {
		return nil, err
	}

	// Whole-batch rejection: item errors (auth/validation) or quota overflow.
	if p.hasItemErrors() || !p.admissible {
		rej := &BatchRejection{Reason: p.reason, Kind: errors.KindQuota}
		batchReason := p.reason
		if p.reason == "" {
			rej.Reason = "one or more items are not admissible"
			batchReason = rej.Reason
		} else {
			batchReason = string(errors.KindQuota) + ": " + p.reason
		}
		for _, pi := range p.items {
			item := rejectItem(pi, batchReason)
			if pi.err != nil && rej.Kind == errors.KindQuota {
				rej.Kind = pi.err.Kind // first classified item error wins
			}
			rej.Items = append(rej.Items, item)
			if item.Status == model.BatchItemFailed {
				o.log.Warn("manifest item rejected",
					"manifest", p.manifestID, "index", pi.index, "name", pi.item.Name,
					"reason", item.Reason)
			}
		}
		o.log.Warn("manifest rejected as a whole; no environment touched",
			"manifest", p.manifestID, "tenant", actor.TenantID,
			"kind", rej.Kind, "reason", rej.Reason)
		return nil, rej
	}

	now := o.now()
	batch := o.st.Batches[p.manifestID]
	if batch == nil {
		batch = &model.Batch{
			ID:        p.manifestID,
			TenantID:  actor.TenantID,
			Actor:     actor.Name,
			CreatedAt: now,
		}
		o.st.Batches[batch.ID] = batch
		o.log.Info("manifest batch declared",
			"manifest", batch.ID, "tenant", actor.TenantID, "actor", actor.Name,
			"items", len(p.items), "netDelta", p.delta)
	} else {
		o.log.Info("manifest replay: reusing persisted batch (idempotent)",
			"manifest", batch.ID, "tenant", actor.TenantID)
	}

	items := make([]*model.BatchItem, 0, len(p.items))
	for _, pi := range p.items {
		var opIDs []string
		switch {
		case pi.err != nil || pi.superseded || pi.kind == model.ChangeNoOp:
			// nothing to do
		case pi.kind == model.ChangeCreate:
			opIDs = o.applyCreateItemLocked(actor, pi, now)
		case pi.kind == model.ChangeUpdate:
			opIDs = o.applyUpdateItemLocked(actor, pi, now)
		case pi.kind == model.ChangeDelete:
			opIDs = o.applyDeleteItemLocked(actor, pi, now)
		}
		items = append(items, batchItemOf(pi, opIDs))
	}
	batch.Items = items
	batch.UpdatedAt = now
	batch.Done = allItemsTerminal(items)

	if err := o.saveLocked(); err != nil {
		return nil, errors.New(errors.KindInternal, err.Error())
	}
	for _, bi := range items {
		o.log.Info("manifest item applied",
			"manifest", batch.ID, "index", bi.Index, "name", bi.Name,
			"action", bi.Action, "envId", bi.EnvID, "status", bi.Status,
			"ops", bi.OpIDs, "note", bi.Note)
	}
	return cloneBatch(batch), nil
}

// applyCreateItemLocked declares a new environment record and queues its
// create op (unless declared Stopped, which materializes nothing).
func (o *Orchestrator) applyCreateItemLocked(actor Actor, pi *plannedItem, now time.Time) []string {
	envID := "env-" + strconv.FormatInt(o.nextSeqLocked(), 10)
	expiry := time.Time{}
	if pi.after.TTLSeconds > 0 {
		expiry = now.Add(time.Duration(pi.after.TTLSeconds) * time.Second)
	}
	env := &model.Environment{
		ID:         envID,
		Spec:       pi.after,
		Desired:    pi.desired,
		Phase:      model.PhasePending,
		Generation: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  expiry,
	}
	o.st.Envs[envID] = env
	pi.env = env
	if pi.desired == model.DesiredRunning {
		op := o.enqueueLocked(model.OpCreate, envID, actor, nil)
		o.log.Info("manifest: environment declared",
			"envId", envID, "name", pi.after.Name, "requested", pi.after.Requested(), "op", op.ID)
		return []string{op.ID}
	}
	o.log.Info("manifest: environment declared stopped (no resources)", "envId", envID, "name", pi.after.Name)
	return nil
}

// applyUpdateItemLocked merges the declared spec into the existing env and
// queues the ops needed to converge: OpUpdate for spec changes, OpStart /
// OpStop for desired-state transitions (including Failed retries and TTL
// renewals of expired envs).
func (o *Orchestrator) applyUpdateItemLocked(actor Actor, pi *plannedItem, now time.Time) []string {
	env := pi.env
	var opIDs []string

	// TTL is a full declaration: re-anchor expiry, and renew an expired env
	// that is declared Running again.
	if pi.after.TTLSeconds > 0 {
		env.ExpiresAt = now.Add(time.Duration(pi.after.TTLSeconds) * time.Second)
	} else {
		env.ExpiresAt = time.Time{}
	}
	renewed := false
	if env.Phase == model.PhaseExpired && pi.desired == model.DesiredRunning {
		env.Phase = model.PhasePending
		renewed = true
		o.log.Info("manifest: expired environment renewed by declaration", "envId", env.ID)
	}

	if pi.after != env.Spec {
		specCopy := pi.after
		op := o.enqueueLocked(model.OpUpdate, env.ID, actor, &specCopy)
		opIDs = append(opIDs, op.ID)
	}

	switch {
	case pi.desired == model.DesiredStopped && env.Desired != model.DesiredStopped:
		env.Desired = model.DesiredStopped
		op := o.enqueueLocked(model.OpStop, env.ID, actor, nil)
		opIDs = append(opIDs, op.ID)
	case pi.desired == model.DesiredRunning && (env.Desired != model.DesiredRunning || renewed):
		env.Desired = model.DesiredRunning
		op := o.enqueueLocked(model.OpStart, env.ID, actor, nil)
		opIDs = append(opIDs, op.ID)
	case pi.desired == model.DesiredRunning && env.Phase == model.PhaseFailed:
		// Retry path: declaration unchanged but the env is Failed; an explicit
		// start resets the failure counters and re-drives materialization.
		op := o.enqueueLocked(model.OpStart, env.ID, actor, nil)
		opIDs = append(opIDs, op.ID)
	}
	env.UpdatedAt = now
	return opIDs
}

// applyDeleteItemLocked tombstones the env and queues its delete op.
func (o *Orchestrator) applyDeleteItemLocked(actor Actor, pi *plannedItem, now time.Time) []string {
	env := pi.env
	env.Desired = model.DesiredDeleted
	env.UpdatedAt = now
	op := o.enqueueLocked(model.OpDelete, env.ID, actor, nil)
	return []string{op.ID}
}

// allItemsTerminal reports whether every item reached a final state.
func allItemsTerminal(items []*model.BatchItem) bool {
	for _, bi := range items {
		if !bi.Terminal() {
			return false
		}
	}
	return true
}

// cloneBatch deep-copies a batch for lock-free reads by callers.
func cloneBatch(b *model.Batch) *model.Batch {
	if b == nil {
		return nil
	}
	cp := *b
	cp.Items = make([]*model.BatchItem, len(b.Items))
	for i, bi := range b.Items {
		itemCopy := *bi
		itemCopy.OpIDs = append([]string(nil), bi.OpIDs...)
		cp.Items[i] = &itemCopy
	}
	return &cp
}

// GetBatch returns the persisted outcome of one manifest application.
// Visible to its actor and to admins; anything else is NotFound.
func (o *Orchestrator) GetBatch(ctx context.Context, actor Actor, batchID string) (*model.Batch, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b := o.st.Batches[batchID]
	if b == nil {
		return nil, errors.New(errors.KindNotFound, "batch not found: "+batchID)
	}
	visible := actor.Role == RoleAdmin ||
		(actor.Role == RoleOwner && b.TenantID == actor.TenantID && b.Actor == actor.Name)
	if !visible {
		return nil, errors.New(errors.KindNotFound, "batch not found: "+batchID)
	}
	return cloneBatch(b), nil
}
