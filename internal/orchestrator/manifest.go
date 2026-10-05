package orchestrator

import (
	"context"
	"strconv"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// This file implements manifest (batch declaration) support:
//
//   - PreviewManifest: read-only change preview. Pure function of the
//     persisted state + manifest; never mutates environments, operations,
//     batches or quota, so repeated previews of the same manifest against an
//     unchanged state are byte-identical.
//   - planLocked: the shared, deterministic decision procedure used by both
//     preview and apply, guaranteeing "what you preview is what you get".
//
// Conflict rule inside one manifest (deterministic and explainable): entries
// targeting the same name are folded to ONE effective entry — Delete always
// wins (matching the global op priority where Delete outranks everything);
// otherwise the LAST Upsert in document order wins. Losing entries are
// reported as NoOp with a note naming the winning entry.

// plannedItem is the fully decided fate of one manifest entry.
type plannedItem struct {
	index    int
	item     model.ManifestItem // normalized effective declaration
	kind     model.ChangeKind
	env      *model.Environment // existing env (nil for creates / absent deletes)
	after    model.EnvSpec      // target spec for upserts
	desired  model.Desired      // target desired state for upserts
	delta    model.Resources    // reservation delta this item contributes
	note     string
	diff     []string
	err      *errors.Error // per-item admission error (validation/auth)
	superseded bool
}

// plan is the result of evaluating a manifest against a state snapshot.
type plan struct {
	manifestID string
	items      []*plannedItem // one per manifest entry, in document order
	tenantID   string
	delta      model.Resources // net reservation change of effective items
	admissible bool
	reason     string
}

// reservationOf returns the reservation an env currently holds.
func reservationOf(e *model.Environment) model.Resources {
	if e == nil || e.Deleted || e.Desired != model.DesiredRunning {
		return model.Resources{}
	}
	return e.Spec.Requested()
}

// planLocked evaluates the manifest against current state. Caller holds o.mu.
// Returns a top-level error only for unauthenticated actors or a structurally
// empty manifest; per-item problems are recorded on the items themselves.
func (o *Orchestrator) planLocked(actor Actor, mf model.Manifest) (*plan, error) {
	if actor.Name == "" || actor.TenantID == "" {
		return nil, errors.New(errors.KindUnauthorized, "unauthenticated actor: name and tenant required")
	}
	if actor.Role != RoleOwner && actor.Role != RoleAdmin {
		return nil, errors.New(errors.KindUnauthorized, "unknown actor role: "+string(actor.Role))
	}
	if len(mf.Items) == 0 {
		return nil, errors.New(errors.KindValidation, "manifest contains no items")
	}

	p := &plan{
		manifestID: mf.ID(actor.TenantID),
		tenantID:   actor.TenantID,
		items:      make([]*plannedItem, len(mf.Items)),
	}

	// 1) Normalize and resolve same-name conflicts: Delete wins, else last wins.
	winner := map[string]int{}
	hasDelete := map[string]bool{}
	for i, raw := range mf.Items {
		it := raw.Normalized()
		p.items[i] = &plannedItem{index: i, item: it}
		if it.Action == model.ManifestDelete {
			hasDelete[it.Name] = true
		}
		// Last occurrence is the candidate winner; a later Delete replaces an
		// earlier Upsert, but a later Upsert never replaces an earlier Delete.
		if _, deleted := hasDelete[it.Name]; !deleted || it.Action == model.ManifestDelete {
			winner[it.Name] = i
		}
	}
	for i, pi := range p.items {
		if pi.item.Name == "" {
			pi.err = errors.New(errors.KindValidation, "manifest item #"+strconv.Itoa(i)+": name is required")
			continue
		}
		w := winner[pi.item.Name]
		if w != i {
			pi.superseded = true
			pi.kind = model.ChangeNoOp
			if hasDelete[pi.item.Name] && pi.item.Action != model.ManifestDelete {
				pi.note = "superseded by item #" + strconv.Itoa(w) + " (Delete wins over Upsert for the same name)"
			} else {
				pi.note = "superseded by item #" + strconv.Itoa(w) + " (later declaration for the same name wins)"
			}
		}
	}

	// 2) Decide each winning item against current state.
	for _, pi := range p.items {
		if pi.err != nil || pi.superseded {
			continue
		}
		o.planItemLocked(actor, pi)
		if pi.err == nil {
			p.delta = p.delta.Add(pi.delta)
		}
	}

	// 3) Net settlement: the whole batch is one atomic reservation change.
	if ok, reason := o.quota.CheckNet(o.st, actor.TenantID, p.delta); !ok {
		p.admissible = false
		p.reason = reason
	} else {
		p.admissible = true
	}
	return p, nil
}

// planItemLocked classifies one effective item and computes its delta.
func (o *Orchestrator) planItemLocked(actor Actor, pi *plannedItem) {
	it := pi.item
	switch it.Action {
	case model.ManifestDelete:
		env := o.findByNameLocked(actor.TenantID, it.Name)
		if env == nil {
			pi.kind = model.ChangeNoOp
			pi.note = "already absent (delete is idempotent)"
			return
		}
		pi.env = env
		if err := o.authorize(actor, env); err != nil {
			pi.err = err.(*errors.Error)
			return
		}
		pi.kind = model.ChangeDelete
		pi.delta = sub(reservationOf(env))
		pi.diff = model.DiffSpecs(&env.Spec, nil)

	case model.ManifestUpsert:
		if it.Desired != model.DesiredRunning && it.Desired != model.DesiredStopped {
			pi.err = errors.New(errors.KindValidation,
				"desired must be Running or Stopped (use action Delete to remove)")
			return
		}
		if err := validateSpec(it.Name, it.Image, it.Replicas, it.Resources, it.TTLSeconds); err != nil {
			pi.err = err.(*errors.Error)
			return
		}
		pi.desired = it.Desired
		env := o.findByNameLocked(actor.TenantID, it.Name)
		if env == nil {
			pi.kind = model.ChangeCreate
			pi.after = model.EnvSpec{
				Name:       it.Name,
				Owner:      actor.Name,
				TenantID:   actor.TenantID,
				Image:      it.Image,
				Replicas:   it.Replicas,
				Resources:  it.Resources,
				TTLSeconds: it.TTLSeconds,
				CreatedAt:  o.now(),
			}
			if pi.desired == model.DesiredRunning {
				pi.delta = pi.after.Requested()
			}
			pi.diff = model.DiffSpecs(nil, &pi.after)
			return
		}
		pi.env = env
		if err := o.authorize(actor, env); err != nil {
			pi.err = err.(*errors.Error)
			return
		}
		pi.after = model.EnvSpec{
			Name:       env.Spec.Name,
			Owner:      env.Spec.Owner, // immutable fields carried over
			TenantID:   env.Spec.TenantID,
			Image:      it.Image,
			Replicas:   it.Replicas,
			Resources:  it.Resources,
			TTLSeconds: it.TTLSeconds,
			CreatedAt:  env.Spec.CreatedAt,
		}
		pi.diff = model.DiffSpecs(&env.Spec, &pi.after)
		if env.Desired != pi.desired {
			pi.diff = append(pi.diff, "desired: "+string(env.Desired)+" -> "+string(pi.desired))
		}
		specSame := pi.after == env.Spec
		desiredSame := env.Desired == pi.desired
		switch {
		case specSame && desiredSame && env.Phase == model.PhaseFailed && pi.desired == model.DesiredRunning:
			// Re-applying the same declaration to a Failed env is the
			// documented retry path: it must do work, not report NoOp.
			pi.kind = model.ChangeUpdate
			pi.note = "retry after failure (spec unchanged, env is Failed)"
		case specSame && desiredSame:
			pi.kind = model.ChangeNoOp
			pi.note = "already matches declaration"
		default:
			pi.kind = model.ChangeUpdate
		}
		oldRes := reservationOf(env)
		newRes := model.Resources{}
		if pi.desired == model.DesiredRunning {
			newRes = pi.after.Requested()
		}
		pi.delta = newRes.Add(sub(oldRes))

	default:
		pi.err = errors.New(errors.KindValidation, "unknown action: "+string(it.Action)+" (want Upsert or Delete)")
	}
}

// sub negates a resource vector.
func sub(r model.Resources) model.Resources {
	return model.Resources{CPUMilli: -r.CPUMilli, MemoryMB: -r.MemoryMB, StorageGB: -r.StorageGB}
}

// hasItemErrors reports whether any item carries an admission error.
func (p *plan) hasItemErrors() bool {
	for _, pi := range p.items {
		if pi.err != nil {
			return true
		}
	}
	return false
}

// preview renders the plan as a read-only ManifestPreview.
func (p *plan) preview() *model.ManifestPreview {
	out := &model.ManifestPreview{
		ManifestID:      p.manifestID,
		Items:           make([]model.ItemPreview, 0, len(p.items)),
		TenantDelta:     p.delta,
		ClusterDelta:    p.delta, // single-tenant manifest: same vector, checked against both limits
		Admissible:      p.admissible && !p.hasItemErrors(),
		AdmissionReason: p.reason,
	}
	for _, pi := range p.items {
		ip := model.ItemPreview{
			Index: pi.index,
			Name:  pi.item.Name,
			Kind:  pi.kind,
			Note:  pi.note,
			Diff:  pi.diff,
			Delta: pi.delta,
		}
		if pi.env != nil {
			ip.EnvID = pi.env.ID
			before := pi.env.Spec
			ip.Before = &before
			ip.DesiredBefore = pi.env.Desired
		}
		if pi.kind == model.ChangeCreate || pi.kind == model.ChangeUpdate {
			after := pi.after
			ip.After = &after
			ip.DesiredAfter = pi.desired
		}
		if pi.err != nil {
			ip.ErrorKind = string(pi.err.Kind)
			ip.Error = pi.err.Message
		}
		if !p.admissible && pi.err == nil && !pi.superseded && pi.kind != model.ChangeNoOp {
			ip.Note = appendNote(ip.Note, "would be rejected with the batch: "+p.reason)
		}
		out.Items = append(out.Items, ip)
	}
	return out
}

func appendNote(note, extra string) string {
	if note == "" {
		return extra
	}
	return note + "; " + extra
}

// PreviewManifest evaluates a manifest against current state WITHOUT any
// side effect: no environment, operation, batch or quota is created, modified
// or deleted. The result is deterministic for a given (state, manifest) pair.
func (o *Orchestrator) PreviewManifest(ctx context.Context, actor Actor, mf model.Manifest) (*model.ManifestPreview, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	p, err := o.planLocked(actor, mf)
	if err != nil {
		return nil, err
	}
	pv := p.preview()
	for _, ip := range pv.Items {
		o.log.Info("manifest preview item",
			"manifest", pv.ManifestID, "index", ip.Index, "name", ip.Name,
			"kind", ip.Kind, "delta", ip.Delta, "note", ip.Note,
			"errorKind", ip.ErrorKind, "error", ip.Error)
	}
	o.log.Info("manifest preview settled",
		"manifest", pv.ManifestID, "tenant", actor.TenantID,
		"tenantDelta", pv.TenantDelta, "clusterDelta", pv.ClusterDelta,
		"admissible", pv.Admissible, "reason", pv.AdmissionReason)
	return pv, nil
}
