// Package reconciler drives environments from their recorded intent toward the
// observed state. Convergence is stepwise: every phase transition is persisted
// around an idempotent backend call, so a crash or restart resumes from the
// last persisted step without ever creating a duplicate resource or going
// backwards. All backend calls are idempotent (Ensure* keyed on resource id);
// Inspect is used to self-heal resources that vanished or changed unexpectedly.
package reconciler

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// PersistFn atomically stores the state; called after every committed step.
type PersistFn func(*domain.State) error

// Reconciler applies commands and self-heals environments against the backend.
type Reconciler struct {
	be  backend.Backend
	clk domain.Clock
	log *log.Logger
}

// New constructs a Reconciler.
func New(be backend.Backend, clk domain.Clock, lg *log.Logger) *Reconciler {
	if lg == nil {
		lg = log.Default()
	}
	return &Reconciler{be: be, clk: clk, log: lg}
}

// activeCommand returns the command that owns the environment for this pass:
// the one with smallest priority, breaking ties by smallest sequence number.
// Deterministic priority is what prevents concurrent submissions from racing:
// e.g. a delete always wins over a queued start regardless of submit timing.
func activeCommand(cmds []domain.Command) *domain.Command {
	if len(cmds) == 0 {
		return nil
	}
	best := 0
	for i := 1; i < len(cmds); i++ {
		if (cmds[i].Type.Priority() < cmds[best].Type.Priority()) ||
			(cmds[i].Type.Priority() == cmds[best].Type.Priority() && cmds[i].Seq < cmds[best].Seq) {
			best = i
		}
	}
	return &cmds[best]
}

// commandsFor returns pointers into st.Commands belonging to env id.
func commandsFor(st *domain.State, id domain.EnvID) []domain.Command {
	var out []domain.Command
	for _, c := range st.Commands {
		if c.EnvID == id {
			out = append(out, c)
		}
	}
	return out
}

// removeCommand drops the command with seq from the persisted queue.
func removeCommand(st *domain.State, seq int64) {
	keep := st.Commands[:0]
	for _, c := range st.Commands {
		if c.Seq != seq {
			keep = append(keep, c)
		}
	}
	st.Commands = keep
}

// settle records the final outcome of an idempotency-keyed command.
func settle(st *domain.State, key string, envID domain.EnvID, t domain.TenantID, envErr error) {
	if key == "" {
		return
	}
	rec := domain.DedupRecord{Key: key, EnvID: envID, Tenant: t, Done: true}
	if envErr != nil {
		if e := domain.AsError(envErr); e != nil {
			rec.ErrKind = e.Kind
			rec.ErrMsg = e.Msg
		} else {
			rec.ErrKind = domain.KindBackend
			rec.ErrMsg = envErr.Error()
		}
	}
	st.Dedup[key] = rec
}

// Pump processes pending commands (deterministic order) plus expiry and
// self-healing, continuing across environments when one hits a transient
// backend wobble. It returns the number of commands fully applied.
func (r *Reconciler) Pump(ctx context.Context, st *domain.State, persist PersistFn) (int, error) {
	applied := 0
	// Deterministic processing order by environment id: makes logs and
	// outcomes reproducible under concurrency.
	ids := make([]domain.EnvID, 0, len(st.Envs))
	for id := range st.Envs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return applied, err
		}
		env := st.Envs[id]
		n, err := r.convergeEnv(ctx, st, env, persist)
		applied += n
		if err != nil {
			r.log.Printf("[reconcile] env=%s paused after transient error (progress persisted): %v", id, err)
		}
	}
	return applied, nil
}

// ReconcileEnv resumes one environment toward its desired state. Idempotent;
// used at startup and from tests targeting a single environment.
func (r *Reconciler) ReconcileEnv(ctx context.Context, st *domain.State, id domain.EnvID, persist PersistFn) error {
	env, ok := st.Envs[id]
	if !ok {
		return domain.NewError(domain.KindNotFound, "env", "environment %s not found", id)
	}
	_, err := r.convergeEnv(ctx, st, env, persist)
	return err
}

// backingID is the single deterministic resource id bound to an environment.
func backingID(env *domain.Environment) string { return "env-" + string(env.ID) }

// convergeEnv drives one environment until it reaches a stable state or hits a
// transient error. It performs at most one backend operation per iteration and
// persists after each committed step. Returns commands fully applied.
func (r *Reconciler) convergeEnv(ctx context.Context, st *domain.State, env *domain.Environment, persist PersistFn) (int, error) {
	applied := 0
	// Bound iterations defensively so a misbehaving backend cannot spin forever.
	for guard := 0; guard < 64; guard++ {
		if err := ctx.Err(); err != nil {
			return applied, err
		}
		cmds := commandsFor(st, env.ID)
		act := activeCommand(cmds)

		// Expiry injection: TTL elapsed and the environment is still live and
		// not already being torn down by an explicit delete.
		if teardownDue(r.clk.Now(), env, act) {
			synth := domain.Command{
				EnvID: env.ID, Tenant: env.Tenant, Type: domain.CmdExpire,
				EnqueuedAt: r.clk.Now(),
			}
			act = &synth
			r.log.Printf("[reconcile] env=%s TTL expired at=%s phase=%s -> injecting expire",
				env.ID, env.ExpiresAt.Format("15:04:05"), env.Phase)
		}

		// Pending command addressing a terminal/tombstoned environment cannot
		// run: record a classified failure and drop it.
		if act != nil && act.Type != domain.CmdExpire && env.Phase.IsTerminal() {
			err := domain.NewError(domain.KindTerminal, "phase_"+string(env.Phase),
				"command %s rejected: environment %s is %s (%s)",
				act.Type, env.ID, env.Phase, env.FailureReason)
			settle(st, act.DedupKey, env.ID, env.Tenant, err)
			removeCommand(st, act.Seq)
			if perr := persist(st); perr != nil {
				return applied, perr
			}
			applied++
			continue
		}

		// One backend interaction (or none if already stable).
		stable, err := r.step(ctx, st, env, act, persist)
		if err != nil {
			return applied, err // transient: state already persisted up to here
		}

		// Synthetic expire: once terminated there is no queued record to clear.
		if act != nil && act.Type == domain.CmdExpire && env.Phase == domain.PhaseTerminated {
			return applied, nil
		}

		// A concrete command completes once the environment matches its intent.
		if act != nil && commandSatisfied(env, act) {
			r.log.Printf("[reconcile] env=%s command seq=%d type=%s satisfied: %s",
				env.ID, act.Seq, act.Type, describe(env))
			settle(st, act.DedupKey, env.ID, env.Tenant, nil)
			removeCommand(st, act.Seq)
			if err := persist(st); err != nil {
				return applied, err
			}
			applied++
			continue
		}

		if stable {
			return applied, nil
		}
	}
	return applied, fmt.Errorf("env %s did not converge within step budget", env.ID)
}

// teardownDue decides whether an expire must be injected now.
func teardownDue(now time.Time, env *domain.Environment, act *domain.Command) bool {
	if !env.ExpiresAt.IsZero() && !now.Before(env.ExpiresAt) {
		switch env.Phase {
		case domain.PhaseTerminated, domain.PhaseFailed, domain.PhaseExpiring, domain.PhaseDeleting:
			return false
		default:
			// An explicit delete already expresses teardown; don't double up.
			if act != nil && act.Type == domain.CmdDelete {
				return false
			}
			return true
		}
	}
	return false
}

// commandSatisfied reports whether the environment reflects the command intent.
// Only called for concrete (queued) commands, never for a synthetic expire.
func commandSatisfied(env *domain.Environment, act *domain.Command) bool {
	if env.Phase.IsTerminal() {
		// Only teardown commands end in termination; other command types are
		// rejected earlier through the terminal branch.
		return act.Type == domain.CmdDelete
	}
	switch act.Type {
	case domain.CmdCreate, domain.CmdUpdate:
		// The declaration is applied when the env is steady and its observed
		// running flag matches the declared desire.
		return (env.Phase == domain.PhaseRunning || env.Phase == domain.PhaseStopped) &&
			env.Running == env.Spec.DesiredRunning
	case domain.CmdStart:
		return env.Phase == domain.PhaseRunning
	case domain.CmdStop:
		return env.Phase == domain.PhaseStopped
	case domain.CmdRecover:
		return env.Phase == domain.PhaseRunning || env.Phase == domain.PhaseStopped
	default:
		return false
	}
}

// touch bumps UpdatedAt and persists, returning any I/O error.
func (r *Reconciler) commit(st *domain.State, env *domain.Environment, persist PersistFn) error {
	env.UpdatedAt = r.clk.Now()
	st.Version++
	if err := persist(st); err != nil {
		return domain.NewError(domain.KindBackend, "persist_failed", "persist state: %v", err)
	}
	return nil
}

// transition logs and applies a phase change.
func (r *Reconciler) transition(env *domain.Environment, to domain.Phase, reason string) {
	r.log.Printf("[reconcile] env=%s phase %s -> %s (%s)", env.ID, env.Phase, to, reason)
	env.Phase = to
}

// fail moves the environment to a terminal failed phase, recording the reason,
// and releases its reservation atomically (all inside one persisted state).
func (r *Reconciler) fail(st *domain.State, env *domain.Environment, distinct, msg string, persist PersistFn) error {
	r.log.Printf("[reconcile] env=%s FAILED distinct=%s reason=%s; releasing reservation %+v",
		env.ID, distinct, msg, env.Reserved)
	env.Phase = domain.PhaseFailed
	env.FailureReason = distinct + ": " + msg
	env.Running = false
	env.Reserved = domain.Resources{}
	return r.commit(st, env, persist)
}

// step performs exactly one backend interaction plus the surrounding persisted
// phase transition. stable=true means the environment needs no further work.
//
// Each branch issues one idempotent Ensure*/Inspect call. A transient backend
// error is returned WITHOUT changing phase, so the next Pump (possibly after a
// process restart) retries the same call; the backend guarantees the retry is
// side-effect-free if the first attempt actually happened.
func (r *Reconciler) step(ctx context.Context, st *domain.State, env *domain.Environment, act *domain.Command, persist PersistFn) (bool, error) {
	id := backingID(env)
	wantDelete := act != nil && (act.Type == domain.CmdDelete || act.Type == domain.CmdExpire)

	// ---- Resume an interrupted teardown even with no queued command -------
	// This is the restart/partial-failure continuation path: the phase alone
	// proves teardown intent must be driven to completion.
	if env.Phase == domain.PhaseExpiring {
		return r.stepTeardown(ctx, st, env, id, domain.CmdExpire, persist)
	}
	if env.Phase == domain.PhaseDeleting {
		return r.stepTeardown(ctx, st, env, id, domain.CmdDelete, persist)
	}

	// ---- Teardown path (highest intent) -----------------------------------
	if wantDelete {
		return r.stepTeardown(ctx, st, env, id, act.Type, persist)
	}

	// ---- Desired intent for non-teardown commands -------------------------
	// Create/update commands may also toggle desired running; start/stop set
	// it explicitly. The desired flag is authoritative for steady state.
	switch env.Phase {
	case domain.PhaseProvisioning:
		// Step 1: ensure the backing resource exists (created as stopped).
		obs, ierr := r.be.Inspect(ctx, id)
		if ierr != nil {
			return false, ierr
		}
		if !obs.Exists {
			r.log.Printf("[reconcile] env=%s creating backing resource requested=%+v", env.ID, env.Spec.Requested)
			if err := r.be.EnsureCreated(ctx, id, env.Spec.Requested); err != nil {
				return false, err // may have partially happened; retry is safe
			}
		}
		env.BackingIDs = []string{id}
		// Reservation is made at acceptance (orchestrator), not here, so that
		// concurrent applications settle quota atomically. Backfill it only if
		// an older/in-progress record had none.
		if (env.Reserved == domain.Resources{}) {
			env.Reserved = env.Spec.Requested
		}
		next := domain.PhaseStopped
		if env.Spec.DesiredRunning {
			next = domain.PhaseStarting
		}
		r.transition(env, next, "resource ensured")
		return false, r.commit(st, env, persist)

	case domain.PhaseStarting:
		obs, err := r.be.Inspect(ctx, id)
		if err != nil {
			return false, err
		}
		if !obs.Exists {
			// Backend lost the resource unexpectedly: self-heal by recreating.
			r.log.Printf("[reconcile] env=%s backing resource missing in starting; recreating", env.ID)
			if err := r.be.EnsureCreated(ctx, id, env.Spec.Requested); err != nil {
				return false, err
			}
		}
		if err := r.be.EnsureStarted(ctx, id); err != nil {
			return false, err
		}
		env.Running = true
		env.BackingIDs = []string{id}
		r.transition(env, domain.PhaseRunning, "resource started")
		return false, r.commit(st, env, persist)

	case domain.PhaseStopping:
		if err := r.be.EnsureStopped(ctx, id); err != nil {
			return false, err
		}
		env.Running = false
		r.transition(env, domain.PhaseStopped, "resource stopped")
		return false, r.commit(st, env, persist)

	case domain.PhaseRunning:
		// Keep observed footprint aligned with declaration (resize is
		// idempotent in the fake backend) and honor desired=false.
		if !env.Spec.DesiredRunning {
			r.transition(env, domain.PhaseStopping, "desire=stopped")
			return false, r.commit(st, env, persist)
		}
		obs, err := r.be.Inspect(ctx, id)
		if err != nil {
			return false, err
		}
		if !obs.Exists {
			r.log.Printf("[reconcile] env=%s resource vanished while running; recreating", env.ID)
			if err := r.be.EnsureCreated(ctx, id, env.Spec.Requested); err != nil {
				return false, err
			}
			r.transition(env, domain.PhaseStarting, "recreate after vanish")
			return false, r.commit(st, env, persist)
		}
		if !obs.Running {
			r.transition(env, domain.PhaseStarting, "observed stopped")
			return false, r.commit(st, env, persist)
		}
		if !obs.Res.Equals(env.Spec.Requested) {
			if err := r.be.EnsureCreated(ctx, id, env.Spec.Requested); err != nil {
				return false, err
			}
		}
		return true, nil // steady

	case domain.PhaseStopped:
		if env.Spec.DesiredRunning {
			r.transition(env, domain.PhaseStarting, "desire=running")
			return false, r.commit(st, env, persist)
		}
		obs, err := r.be.Inspect(ctx, id)
		if err != nil {
			return false, err
		}
		if !obs.Exists {
			// Resource gone while stopped: recreate but stay stopped (footprint retained).
			r.log.Printf("[reconcile] env=%s resource vanished while stopped; recreating", env.ID)
			if err := r.be.EnsureCreated(ctx, id, env.Spec.Requested); err != nil {
				return false, err
			}
		}
		return true, nil // steady & stopped

	default:
		// Terminal/expiring/deleting/failed are handled on dedicated paths.
		return true, nil
	}
}

// stepTeardown drives delete/expire: running -> stopped -> absent -> terminated.
func (r *Reconciler) stepTeardown(ctx context.Context, st *domain.State, env *domain.Environment, id string, kind domain.CommandType, persist PersistFn) (bool, error) {
	teardownPhase := domain.PhaseDeleting
	reason := "delete requested"
	if kind == domain.CmdExpire {
		teardownPhase = domain.PhaseExpiring
		reason = "ttl expired"
	}

	switch env.Phase {
	case domain.PhaseRunning, domain.PhaseStarting, domain.PhaseProvisioning:
		if env.Phase != teardownPhase {
			r.transition(env, teardownPhase, reason)
			return false, r.commit(st, env, persist)
		}
		// First stop the resource in an orderly way.
		if err := r.be.EnsureStopped(ctx, id); err != nil {
			return false, err
		}
		env.Running = false
		r.log.Printf("[reconcile] env=%s stopped as part of teardown", env.ID)
		return false, r.commit(st, env, persist)

	case domain.PhaseStopped, domain.PhaseStopping:
		if env.Phase != teardownPhase {
			r.transition(env, teardownPhase, reason)
			return false, r.commit(st, env, persist)
		}
		obs, err := r.be.Inspect(ctx, id)
		if err != nil {
			return false, err
		}
		if obs.Exists && obs.Running {
			if err := r.be.EnsureStopped(ctx, id); err != nil {
				return false, err
			}
		}
		if err := r.be.EnsureDeleted(ctx, id); err != nil {
			return false, err
		}
		env.BackingIDs = nil
		// Quota release is atomic with reaching the terminal phase.
		env.Reserved = domain.Resources{}
		env.Running = false
		r.transition(env, domain.PhaseTerminated, "resources removed; quota released")
		return false, r.commit(st, env, persist)

	case domain.PhaseExpiring, domain.PhaseDeleting:
		// Resume an interrupted teardown: ensure stopped, then deleted.
		obs, err := r.be.Inspect(ctx, id)
		if err != nil {
			return false, err
		}
		if obs.Exists && obs.Running {
			if err := r.be.EnsureStopped(ctx, id); err != nil {
				return false, err
			}
			env.Running = false
			return false, r.commit(st, env, persist)
		}
		if obs.Exists {
			if err := r.be.EnsureDeleted(ctx, id); err != nil {
				return false, err
			}
		}
		env.BackingIDs = nil
		env.Reserved = domain.Resources{}
		env.Running = false
		r.transition(env, domain.PhaseTerminated, "teardown resumed and completed")
		return false, r.commit(st, env, persist)

	default:
		// Already terminated: idempotent no-op.
		return true, nil
	}
}

// describe prints a concise env summary for logs.
func describe(env *domain.Environment) string {
	var b strings.Builder
	fmt.Fprintf(&b, "env=%s tenant=%s phase=%s running=%t desired=%t reserved=%+v requested=%+v",
		env.ID, env.Tenant, env.Phase, env.Running, env.Spec.DesiredRunning, env.Reserved, env.Spec.Requested)
	return b.String()
}
