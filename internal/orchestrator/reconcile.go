package orchestrator

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// runLoop is the resumable desired-vs-observed reconciliation loop.
// Every tick performs one bounded convergence pass; all progress is
// persisted, so interruption (process kill / restart) simply resumes on the
// next process's first pass.
func (o *Orchestrator) runLoop() {
	defer close(o.doneCh)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	ctx := context.Background()
	for {
		select {
		case <-o.stopCh:
			return
		case <-ticker.C:
			o.reconcileOnce(ctx)
		}
	}
}

// nextPendingLocked returns unfinished operations in deterministic conflict
// order: highest priority (smallest number) first, ties by FIFO seq.
func (o *Orchestrator) nextPendingLocked() []*model.Operation {
	var pending []*model.Operation
	for _, op := range o.st.Operations {
		if !op.Done {
			pending = append(pending, op)
		}
	}
	sort.SliceStable(pending, func(i, j int) bool {
		if pending[i].Priority != pending[j].Priority {
			return pending[i].Priority < pending[j].Priority
		}
		return pending[i].Seq < pending[j].Seq
	})
	return pending
}

// reconcileOnce processes the priority-ordered queue then converges drift
// (TTL expiry, missing/extra allocations) for every environment.
func (o *Orchestrator) reconcileOnce(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := o.now()
	changed := false

	// 1) Queued intents, deterministic global priority order.
	for _, op := range o.nextPendingLocked() {
		if ctx.Err() != nil {
			return
		}
		env := o.st.Envs[op.EnvID]
		if env == nil {
			op.Done = true
			op.Rejected = true
			op.Message = "environment record missing"
			changed = true
			continue
		}
		switch op.Kind {
		case model.OpCreate:
			changed = o.applyCreate(ctx, env, op) || changed
		case model.OpUpdate:
			changed = o.applyUpdate(ctx, env, op) || changed
		case model.OpStart:
			changed = o.applyStart(ctx, env, op) || changed
		case model.OpStop:
			changed = o.applyStop(ctx, env, op) || changed
		case model.OpDelete:
			changed = o.applyDelete(ctx, env, op) || changed
		}
	}

	// 2) Autonomous drift convergence for each active environment.
	ids := make([]string, 0, len(o.st.Envs))
	for id := range o.st.Envs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		env := o.st.Envs[id]
		if env.Deleted {
			continue
		}
		if env.IsExpired(now) {
			changed = o.reclaimExpired(ctx, env) || changed
			continue
		}
		switch env.Desired {
		case model.DesiredRunning:
			changed = o.convergeRunning(ctx, env) || changed
		case model.DesiredStopped:
			changed = o.convergeStopped(ctx, env) || changed
		}
	}

	// 3) Reflect finished operations into manifest batch items.
	changed = o.updateBatchesLocked() || changed

	// 4) Prune finished operations to keep the document bounded.
	if pruned := o.pruneDoneLocked(); pruned > 0 {
		changed = true
	}

	if changed {
		if err := o.saveLocked(); err != nil {
			o.log.Error("persist failed (will retry next tick)", "error", err)
		}
	}
}

// markDone finishes an operation with optional explanation.
func markDone(op *model.Operation, rejected bool, msg string) {
	op.Done = true
	op.Rejected = rejected
	op.Message = msg
}

// desiredAllocations builds the deterministic target allocation set for a
// spec at the given generation: envID-gen-0..replicas-1.
func desiredAllocations(env *model.Environment, gen int64) []model.Allocation {
	allocs := make([]model.Allocation, 0, env.Spec.Replicas)
	for i := 0; i < env.Spec.Replicas; i++ {
		id := env.ID + "-g" + strconv.FormatInt(gen, 10) + "-" + strconv.Itoa(i)
		allocs = append(allocs, model.Allocation{
			ID:        id,
			EnvID:     env.ID,
			TenantID:  env.Spec.TenantID,
			Image:     env.Spec.Image,
			Resources: env.Spec.Resources,
		})
	}
	return allocs
}

// allocIndex returns a set of live allocation IDs.
func allocIndex(env *model.Environment) map[string]bool {
	m := make(map[string]bool, len(env.Allocations))
	for _, a := range env.Allocations {
		m[a.ID] = true
	}
	return m
}

// releaseOne deletes one allocation from the backend and the record.
// Returns: removed(true/false), err(backend failure).
func (o *Orchestrator) releaseOne(ctx context.Context, env *model.Environment, idx int) (bool, error) {
	a := env.Allocations[idx]
	if err := o.backend.DeleteAllocation(ctx, a.ID); err != nil {
		return false, err
	}
	env.Allocations = append(env.Allocations[:idx], env.Allocations[idx+1:]...)
	o.log.Info("allocation released", "envId", env.ID, "allocId", a.ID, "phase", env.Phase)
	return true, nil
}

// releaseAll releases every live allocation, tolerating interruption.
// Returns true when at least one release happened; false (with env intact)
// on backend failure so the next pass resumes.
func (o *Orchestrator) releaseAll(ctx context.Context, env *model.Environment) (bool, error) {
	changed := false
	for len(env.Allocations) > 0 {
		removed, err := o.releaseOne(ctx, env, 0)
		if err != nil {
			o.log.Warn("release failed; will resume next pass", "envId", env.ID, "error", err)
			return changed, err
		}
		changed = changed || removed
	}
	return changed, nil
}

// ---- operation handlers -------------------------------------------------
// Each handler advances one op by a bounded amount and persists on every
// progress step; a crash mid-handler resumes exactly where it stopped.

func (o *Orchestrator) applyCreate(ctx context.Context, env *model.Environment, op *model.Operation) bool {
	if env.Desired == model.DesiredDeleted || env.Deleted {
		markDone(op, false, "superseded by delete")
		return true
	}
	changed := false
	if env.Phase != model.PhaseStarting {
		env.Phase = model.PhaseStarting
		changed = true
	}
	target := desiredAllocations(env, env.Generation)
	for _, a := range target {
		if hasAlloc(env, a.ID) {
			continue
		}
		// The spec's reservation was admitted atomically in CreateEnvironment
		// (or via an update), so materialization never needs to re-reserve
		// quota here; only transient backend failures must be retried.
		if err := o.backend.CreateAllocation(ctx, a); err != nil {
			env.Attempts++
			if env.Attempts >= MaxBackendAttempts {
				env.FailReason = "backend create failed repeatedly: " + err.Error()
				env.Phase = model.PhaseFailed
				markDone(op, true, env.FailReason)
				o.log.Error("create exhausted retries -> Failed", "envId", env.ID, "reason", env.FailReason)
				return true
			}
			o.log.Warn("create backend blip; resumable retry", "envId", env.ID, "allocId", a.ID, "attempt", env.Attempts, "error", err)
			return changed
		}
		env.Allocations = append(env.Allocations, a)
		env.Attempts = 0
		changed = true
		o.log.Info("allocation materialized", "envId", env.ID, "allocId", a.ID, "live", len(env.Allocations), "/", env.Spec.Replicas)
	}
	if len(env.Allocations) == env.Spec.Replicas {
		env.Phase = model.PhaseRunning
		markDone(op, false, "running")
		changed = true
		o.log.Info("environment Running", "envId", env.ID, "allocations", len(env.Allocations))
	}
	return changed
}

func (o *Orchestrator) applyUpdate(ctx context.Context, env *model.Environment, op *model.Operation) bool {
	if env.Desired == model.DesiredDeleted || env.Deleted {
		markDone(op, false, "superseded by delete")
		return true
	}
	if op.NewSpec == nil {
		markDone(op, true, "update carried no spec")
		return true
	}
	// Refresh TTL anchor using update time.
	newSpec := *op.NewSpec

	oldReserved := env.Spec.Requested() // env is DesiredRunning here unless stopped
	if env.Desired != model.DesiredRunning {
		oldReserved = model.Resources{}
	}
	newReserved := newSpec.Requested()
	if env.Desired != model.DesiredRunning {
		newReserved = model.Resources{}
	}
	// Re-validate at apply time because admin quota/capacity shrink or other
	// reservations may have landed after the API admitted the update.
	if ok, reason := o.quota.CheckChange(o.st, env.Spec.TenantID, oldReserved, newReserved); !ok {
		env.Attempts++
		if env.Attempts >= MaxBackendAttempts {
			env.FailReason = "quota insufficient for update: " + reason
			env.Phase = model.PhaseFailed
			markDone(op, true, env.FailReason)
			o.log.Error("update exhausted retries -> Failed", "envId", env.ID, "reason", env.FailReason)
			return true
		}
		o.log.Warn("update blocked by quota; retrying", "envId", env.ID, "attempt", env.Attempts, "reason", reason)
		return false
	}

	// A change in resources/image/replicas requires a new generation so the
	// allocation set is deterministic and replacements are idempotent.
	needNewGen := newSpec.Image != env.Spec.Image ||
		newSpec.Resources != env.Spec.Resources ||
		newSpec.Replicas != env.Spec.Replicas

	if needNewGen {
		env.Generation++
		o.log.Info("spec change: new generation", "envId", env.ID, "generation", env.Generation)
	}
	env.Spec = newSpec
	env.FailReason = ""
	env.Attempts = 0
	markDone(op, false, "spec applied; converging")
	o.log.Info("update applied", "envId", env.ID, "replicas", env.Spec.Replicas, "image", env.Spec.Image)
	return true
}

func (o *Orchestrator) applyStart(ctx context.Context, env *model.Environment, op *model.Operation) bool {
	// Only a Stopped/Expired env (whose reservation was released) needs to
	// re-acquire quota. A Failed-but-desired-Running env still holds its
	// reservation and simply retries materialization.
	needsReservation := env.Phase == model.PhaseStopped || env.Phase == model.PhaseExpired
	if needsReservation {
		req := env.Spec.Requested()
		var ok bool
		var reason string
		if env.Desired == model.DesiredRunning {
			// The API already recorded the reservation in Desired; re-verify
			// the TOTAL still fits (same pattern as convergeRunning) instead
			// of adding the request a second time.
			ok, reason = o.quota.CheckChange(o.st, env.Spec.TenantID, req, req)
		} else {
			ok, reason = o.quota.Fits(o.st, env.Spec.TenantID, req)
		}
		if !ok {
			env.Attempts++
			if env.Attempts >= MaxBackendAttempts {
				env.FailReason = "cannot start: quota insufficient: " + reason
				env.Phase = model.PhaseFailed
				markDone(op, true, env.FailReason)
				o.log.Error("start exhausted retries -> Failed", "envId", env.ID, "reason", env.FailReason)
				return true
			}
			o.log.Warn("start blocked by quota; retrying", "envId", env.ID, "attempt", env.Attempts, "reason", reason)
			return false
		}
	}
	env.Desired = model.DesiredRunning
	if env.Phase == model.PhaseFailed || env.Phase == model.PhaseStopped || env.Phase == model.PhaseExpired {
		env.Phase = model.PhaseStarting
		env.FailReason = ""
		env.Attempts = 0
	}
	markDone(op, false, "desired running")
	o.log.Info("start intent recorded", "envId", env.ID, "phase", env.Phase)
	return true
}

func (o *Orchestrator) applyStop(ctx context.Context, env *model.Environment, op *model.Operation) bool {
	env.Desired = model.DesiredStopped
	if env.Phase != model.PhaseStopped && env.Phase != model.PhaseStopping {
		env.Phase = model.PhaseStopping
	}
	changed, err := o.releaseAll(ctx, env)
	if err != nil {
		// op stays pending; deterministic priority ensures stop keeps winning.
		return changed
	}
	env.Phase = model.PhaseStopped
	env.Attempts = 0
	markDone(op, false, "stopped; resources released")
	o.log.Info("environment Stopped", "envId", env.ID)
	return true
}

func (o *Orchestrator) applyDelete(ctx context.Context, env *model.Environment, op *model.Operation) bool {
	env.Desired = model.DesiredDeleted
	if env.Phase != model.PhaseDeleting {
		env.Phase = model.PhaseDeleting
	}
	changed, err := o.releaseAll(ctx, env)
	if err != nil {
		return changed // resume release next pass
	}
	env.Deleted = true
	env.FailReason = ""
	markDone(op, false, "deleted; quota released")
	o.log.Info("environment deleted (tombstone hidden from queries)", "envId", env.ID, "tenant", env.Spec.TenantID)
	return true
}

// ---- drift convergence --------------------------------------------------

// convergeRunning ensures a Running-desired env has exactly its target set.
func (o *Orchestrator) convergeRunning(ctx context.Context, env *model.Environment) bool {
	if env.Phase == model.PhaseFailed {
		return false // requires explicit start/update to retry
	}
	target := desiredAllocations(env, env.Generation)
	want := allocIndexFromList(target)
	changed := false

	// Release stale-generation or over-replica allocations first (frees quota).
	kept := env.Allocations[:0]
	for _, a := range env.Allocations {
		if want[a.ID] {
			kept = append(kept, a)
			continue
		}
		if err := o.backend.DeleteAllocation(ctx, a.ID); err != nil {
			o.log.Warn("drift: stale release failed; retry", "envId", env.ID, "allocId", a.ID, "error", err)
			kept = append(kept, a)
			continue
		}
		changed = true
		o.log.Info("drift: stale allocation released", "envId", env.ID, "allocId", a.ID)
	}
	env.Allocations = kept

	for _, a := range target {
		if hasAlloc(env, a.ID) {
			continue
		}
		// Reservations were admitted at declaration time. Only an admin
		// quota shrink in the meantime can block convergence now.
		oldReserved := env.Spec.Requested()
		if ok, reason := o.quota.CheckChange(o.st, env.Spec.TenantID, oldReserved, oldReserved); !ok {
			if env.Phase != model.PhaseStarting {
				env.Phase = model.PhaseStarting
				changed = true
			}
			env.Attempts++
			if env.Attempts >= MaxBackendAttempts {
				env.FailReason = "quota/capacity shrunk below reservation: " + reason
				env.Phase = model.PhaseFailed
				changed = true
				o.log.Error("convergence failed after quota shrink", "envId", env.ID, "reason", env.FailReason)
				return changed
			}
			o.log.Warn("drift: reservation exceeds shrunken quota; retrying", "envId", env.ID, "attempt", env.Attempts, "reason", reason)
			return changed
		}
		if err := o.backend.CreateAllocation(ctx, a); err != nil {
			o.log.Warn("drift: create blip; retry", "envId", env.ID, "allocId", a.ID, "error", err)
			if env.Phase != model.PhaseStarting {
				env.Phase = model.PhaseStarting
			}
			return changed
		}
		env.Allocations = append(env.Allocations, a)
		changed = true
	}

	if len(env.Allocations) == env.Spec.Replicas && env.Phase != model.PhaseRunning {
		env.Phase = model.PhaseRunning
		env.FailReason = ""
		env.Attempts = 0
		changed = true
		o.log.Info("drift converged -> Running", "envId", env.ID, "allocations", len(env.Allocations))
	}
	return changed
}

// convergeStopped guarantees no allocations remain for a Stopped-desired env.
func (o *Orchestrator) convergeStopped(ctx context.Context, env *model.Environment) bool {
	if len(env.Allocations) == 0 {
		if env.Phase != model.PhaseStopped {
			env.Phase = model.PhaseStopped
			return true
		}
		return false
	}
	if env.Phase != model.PhaseStopping {
		env.Phase = model.PhaseStopping
	}
	changed, err := o.releaseAll(ctx, env)
	if err != nil {
		return changed
	}
	env.Phase = model.PhaseStopped
	o.log.Info("drift converged -> Stopped", "envId", env.ID)
	return true
}

// reclaimExpired handles TTL expiry like a stop, recording the explainable
// Expired terminal phase and releasing all quota/resource usage.
func (o *Orchestrator) reclaimExpired(ctx context.Context, env *model.Environment) bool {
	if env.Phase != model.PhaseExpired && env.Phase != model.PhaseStopping {
		o.log.Info("TTL expired: reclaiming", "envId", env.ID, "expiresAt", env.ExpiresAt)
		env.Phase = model.PhaseStopping
	}
	env.Desired = model.DesiredStopped
	changed, err := o.releaseAll(ctx, env)
	if err != nil {
		return changed
	}
	if env.Phase != model.PhaseExpired {
		env.Phase = model.PhaseExpired
		changed = true
	}
	o.log.Info("environment Expired; resources reclaimed", "envId", env.ID)
	return changed
}

// ---- recovery & pruning -------------------------------------------------

// recoverLocked repairs state after a process restart: interruptions may have
// left phases mid-transition. Desired + live allocations determine the
// consistent target phase; pending ops remain and drive the rest.
func (o *Orchestrator) recoverLocked() {
	for _, env := range o.st.Envs {
		live := len(env.Allocations)
		// A control-plane restart is itself a recovery event: transient
		// failure counters reset and a Failed env that still desires Running
		// gets another chance (the new process may see a healthy backend).
		if env.Phase == model.PhaseFailed && env.Desired == model.DesiredRunning {
			env.Phase = model.PhaseStarting
			env.FailReason = ""
			env.Attempts = 0
			// If the original Create op was pruned after failing, requeue one
			// so convergence has an intent to drive; drift would otherwise
			// also converge, but the op keeps the record explainable.
		}
		switch {
		case env.Deleted:
			// Release resumed lazily by drift / pending delete op.
			if live > 0 && env.Phase != model.PhaseDeleting {
				env.Phase = model.PhaseDeleting
			}
		case env.IsExpired(o.now()):
			if env.Phase != model.PhaseExpired {
				env.Phase = model.PhaseStopping
			}
		case env.Desired == model.DesiredRunning:
			if live >= env.Spec.Replicas && env.Spec.Replicas > 0 {
				env.Phase = model.PhaseRunning
			} else if env.Phase == model.PhaseRunning || env.Phase == model.PhasePending ||
				env.Phase == model.PhaseStarting {
				env.Phase = model.PhaseStarting
			}
		case env.Desired == model.DesiredStopped:
			if live == 0 {
				env.Phase = model.PhaseStopped
			} else {
				env.Phase = model.PhaseStopping
			}
		}
		o.log.Info("recover inspected",
			"envId", env.ID, "desired", env.Desired, "phase", env.Phase,
			"liveAllocations", live, "pendingOpsForEnv", o.pendingOpForLocked(env.ID))
	}
	// Manifest batches survive restarts: any pending item whose operation is
	// no longer on record is re-derived so the apply keeps converging.
	for _, b := range o.st.Batches {
		if b.Done {
			continue
		}
		for _, bi := range b.Items {
			if bi.Terminal() {
				continue
			}
			missing := len(bi.OpIDs) == 0
			for _, opID := range bi.OpIDs {
				if o.findOpLocked(opID) == nil {
					missing = true
					break
				}
			}
			if missing {
				o.log.Info("recover: manifest item missing its operation; re-deriving",
					"manifest", b.ID, "index", bi.Index, "name", bi.Name)
				o.recoverBatchItemLocked(b, bi)
			}
		}
	}
}

// pruneDoneLocked removes finished operations that are not needed for
// restart semantics, keeping the persisted document bounded. Operations
// referenced by a still-pending manifest batch item are retained until the
// batch item has observed their outcome.
func (o *Orchestrator) pruneDoneLocked() int {
	protected := map[string]bool{}
	for _, b := range o.st.Batches {
		if b.Done {
			continue
		}
		for _, bi := range b.Items {
			if bi.Terminal() {
				continue
			}
			for _, id := range bi.OpIDs {
				protected[id] = true
			}
		}
	}
	keep := o.st.Operations[:0]
	n := 0
	for _, op := range o.st.Operations {
		if op.Done && !protected[op.ID] {
			n++
			continue
		}
		keep = append(keep, op)
	}
	o.st.Operations = keep
	return n
}

// findOpLocked locates an operation by ID (pending or not-yet-pruned).
func (o *Orchestrator) findOpLocked(id string) *model.Operation {
	for _, op := range o.st.Operations {
		if op.ID == id {
			return op
		}
	}
	return nil
}

// updateBatchesLocked folds operation outcomes into manifest batch items so
// every apply stays explainable: which items converged, which failed and why.
func (o *Orchestrator) updateBatchesLocked() bool {
	changed := false
	for _, b := range o.st.Batches {
		if b.Done {
			continue
		}
		for _, bi := range b.Items {
			if bi.Terminal() {
				continue
			}
			allDone, missing := true, false
			for _, opID := range bi.OpIDs {
				op := o.findOpLocked(opID)
				if op == nil {
					missing = true
					break
				}
				if !op.Done {
					allDone = false
					break
				}
				if op.Rejected {
					bi.Status = model.BatchItemFailed
					bi.Reason = op.Message
					changed = true
					o.log.Warn("manifest item failed",
						"manifest", b.ID, "index", bi.Index, "name", bi.Name,
						"envId", bi.EnvID, "reason", bi.Reason)
					break
				}
			}
			if bi.Terminal() {
				continue
			}
			if missing {
				// Defensive: prune protection makes this unreachable, but a
				// hand-edited or corrupted document must not wedge the batch.
				o.recoverBatchItemLocked(b, bi)
				changed = true
				continue
			}
			if allDone && len(bi.OpIDs) > 0 {
				bi.Status = model.BatchItemApplied
				changed = true
				o.log.Info("manifest item converged",
					"manifest", b.ID, "index", bi.Index, "name", bi.Name, "envId", bi.EnvID)
			}
		}
		if !b.Done && allItemsTerminal(b.Items) {
			b.Done = true
			changed = true
			o.log.Info("manifest batch complete", "manifest", b.ID, "items", len(b.Items))
		}
		if changed {
			b.UpdatedAt = o.now()
		}
	}
	return changed
}

// recoverBatchItemLocked re-derives a pending batch item whose operation
// record is missing: satisfied intents are closed, unsatisfied ones get a
// fresh operation so the batch keeps converging after any interruption.
func (o *Orchestrator) recoverBatchItemLocked(b *model.Batch, bi *model.BatchItem) {
	env := o.envLocked(bi.EnvID)
	if bi.Action == model.ManifestDelete {
		if env == nil {
			bi.Status = model.BatchItemApplied
			bi.Note = appendNote(bi.Note, "confirmed absent after recovery")
			return
		}
	} else if env != nil && bi.Spec != nil && env.Spec == *bi.Spec &&
		env.Desired == bi.Desired && !phaseTransitioning(env.Phase) && env.Phase != model.PhaseFailed {
		bi.Status = model.BatchItemApplied
		bi.Note = appendNote(bi.Note, "confirmed converged after recovery")
		return
	}
	if env == nil {
		bi.Status = model.BatchItemFailed
		bi.Reason = "environment record missing after recovery; cannot re-drive item"
		o.log.Warn("manifest item unrecoverable", "manifest", b.ID, "index", bi.Index, "name", bi.Name)
		return
	}
	actor := Actor{Name: b.Actor, TenantID: b.TenantID, Role: RoleOwner}
	var op *model.Operation
	switch bi.Action {
	case model.ManifestDelete:
		env.Desired = model.DesiredDeleted
		op = o.enqueueLocked(model.OpDelete, env.ID, actor, nil)
	case model.ManifestUpsert:
		if bi.Spec != nil && env.Spec != *bi.Spec {
			specCopy := *bi.Spec
			op = o.enqueueLocked(model.OpUpdate, env.ID, actor, &specCopy)
		}
		if bi.Desired == model.DesiredStopped && env.Desired != model.DesiredStopped {
			env.Desired = model.DesiredStopped
			op = o.enqueueLocked(model.OpStop, env.ID, actor, nil)
		}
		if bi.Desired == model.DesiredRunning && (env.Desired != model.DesiredRunning || env.Phase == model.PhaseFailed) {
			env.Desired = model.DesiredRunning
			op = o.enqueueLocked(model.OpStart, env.ID, actor, nil)
		}
	}
	if op != nil {
		bi.OpIDs = []string{op.ID}
		o.log.Info("manifest item re-driven after recovery",
			"manifest", b.ID, "index", bi.Index, "name", bi.Name, "op", op.ID)
	} else {
		bi.Status = model.BatchItemApplied
		bi.Note = appendNote(bi.Note, "no residual work after recovery")
	}
}

// ---- small helpers ------------------------------------------------------

func allocIndexFromList(allocs []model.Allocation) map[string]bool {
	m := make(map[string]bool, len(allocs))
	for _, a := range allocs {
		m[a.ID] = true
	}
	return m
}

func hasAlloc(env *model.Environment, id string) bool {
	for _, a := range env.Allocations {
		if a.ID == id {
			return true
		}
	}
	return false
}
