package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// maxTTL bounds validity duration; 0 means "never expire".
const maxTTL = 30 * 24 * time.Hour

// authorizeRead permits admin (any tenant) or same-tenant principals.
func authorizeRead(p domain.Principal, e *domain.Environment) error {
	if p.Role == domain.RoleAdmin {
		return nil
	}
	if p.Tenant != e.Tenant {
		return domain.NewError(domain.KindForbidden, "cross_tenant",
			"principal tenant %s cannot access environment of tenant %s", p.Tenant, e.Tenant)
	}
	return nil
}

// authorizeWrite permits admin or the environment owner within the same tenant.
func authorizeWrite(p domain.Principal, e *domain.Environment) error {
	if p.Role == domain.RoleAdmin {
		return nil
	}
	if p.Tenant != e.Tenant {
		return domain.NewError(domain.KindForbidden, "cross_tenant",
			"principal tenant %s cannot mutate environment of tenant %s", p.Tenant, e.Tenant)
	}
	if p.User != e.Spec.Owner {
		return domain.NewError(domain.KindForbidden, "not_owner",
			"user %s is not the owner %s of environment %s", p.User, e.Spec.Owner, e.ID)
	}
	return nil
}

func validatePrincipal(p domain.Principal) error {
	if p.User == "" {
		return domain.NewError(domain.KindUnauthorized, "anonymous",
			"an authenticated principal with a user is required")
	}
	if p.Role != domain.RoleUser && p.Role != domain.RoleAdmin {
		return domain.NewError(domain.KindUnauthorized, "bad_role", "unknown role %q", p.Role)
	}
	// Admin is global and may operate across tenants (tenant may be empty);
	// ordinary users must be scoped to exactly one tenant.
	if p.Role == domain.RoleUser && p.Tenant == "" {
		return domain.NewError(domain.KindUnauthorized, "tenant_required",
			"non-admin principal must carry a tenant")
	}
	return nil
}

func validateResources(r domain.Resources) error {
	if r.CPU < 0 || r.Memory < 0 || r.Storage < 0 {
		return domain.NewError(domain.KindValidation, "negative_resources",
			"resources must be non-negative: %+v", r)
	}
	if r == (domain.Resources{}) {
		return domain.NewError(domain.KindValidation, "zero_resources",
			"at least one resource dimension must be > 0")
	}
	return nil
}

func validateName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return domain.NewError(domain.KindValidation, "name_required", "environment name is required")
	}
	if len(name) > 64 {
		return domain.NewError(domain.KindValidation, "name_too_long", "name exceeds 64 chars")
	}
	return nil
}

func validateTTL(ttl time.Duration) error {
	if ttl < 0 {
		return domain.NewError(domain.KindValidation, "negative_ttl", "ttl must not be negative")
	}
	if ttl > maxTTL {
		return domain.NewError(domain.KindValidation, "ttl_too_long", "ttl %s exceeds max %s", ttl, maxTTL)
	}
	return nil
}

// nameTaken reports a name collision against another *live* environment in the
// same tenant. Terminated tombstones do not block name reuse.
func (o *Orchestrator) nameTaken(tenant domain.TenantID, name string, except domain.EnvID) bool {
	for _, e := range o.st.Envs {
		if e.Tenant == tenant && e.ID != except && e.Spec.Name == name && !e.Phase.IsTerminal() {
			return true
		}
	}
	return false
}

func newEnvID() domain.EnvID {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return domain.EnvID(hex.EncodeToString(b[:]))
}

// CreateRequest is a declarative create submission.
type CreateRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	Name           string
	Resources      domain.Resources
	TTL            time.Duration
	Start          bool
}

// Create accepts (and reconciles) a new environment declaration. Repeating the
// request with the same IdempotencyKey returns the original outcome without
// creating a second environment or repeating side effects.
func (o *Orchestrator) Create(ctx context.Context, req CreateRequest) (*domain.Environment, error) {
	if err := validatePrincipal(req.Principal); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(req.Name)
	if err := validateName(name); err != nil {
		return nil, err
	}
	if err := validateResources(req.Resources); err != nil {
		return nil, err
	}
	if err := validateTTL(req.TTL); err != nil {
		return nil, err
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	key := dedupKey(req.Principal, req.IdempotencyKey)
	if req.IdempotencyKey != "" {
		if rec, ok := o.st.Dedup[key]; ok {
			o.log.Printf("[submit] duplicate create key=%q -> recorded outcome env=%s errKind=%s",
				key, rec.EnvID, rec.ErrKind)
			return o.recordedOutcome(rec)
		}
	}

	if o.nameTaken(req.Principal.Tenant, name, "") {
		err := domain.NewError(domain.KindDuplicate, "name_in_use",
			"tenant %s already has a live environment named %q", req.Principal.Tenant, name)
		o.recordFailure(key, "", req.Principal.Tenant, err)
		return nil, err
	}
	// Atomic quota settlement: check against all currently reserved resources
	// while holding the global lock; no other create can interleave.
	if err := o.qm.CheckNew(o.st, req.Principal.Tenant, req.Resources); err != nil {
		o.log.Printf("[submit] create rejected by quota name=%s req=%+v reason=%v", name, req.Resources, err)
		o.recordFailure(key, "", req.Principal.Tenant, err)
		return nil, err
	}

	now := o.clk.Now()
	id := newEnvID()
	env := &domain.Environment{
		ID:     id,
		Tenant: req.Principal.Tenant,
		Spec: domain.Spec{
			Name: name, Owner: req.Principal.User, Requested: req.Resources,
			TTL: req.TTL, DesiredRunning: req.Start,
		},
		// Reservation is taken at acceptance, so concurrent requests cannot
		// oversubscribe before resources physically exist.
		Reserved:  req.Resources,
		Phase:     domain.PhaseProvisioning,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if req.TTL > 0 {
		env.ExpiresAt = now.Add(req.TTL)
	}
	o.st.Envs[id] = env

	c := domain.Command{
		Seq: o.nextSeq(), EnvID: id, Tenant: env.Tenant,
		Type: domain.CmdCreate, DedupKey: key, EnqueuedAt: now,
	}
	if err := o.enqueueAndPump(ctx, c); err != nil {
		// Transient backend wobble: intent + reservation are persisted and a
		// later pump resumes. Return the current record, not an acceptance.
		return cloneEnv(env), err
	}
	o.log.Printf("[submit] create accepted id=%s name=%s phase=%s reserved=%+v quotaUsed=%+v",
		id, name, env.Phase, env.Reserved, o.qm.Usage(o.st, env.Tenant))
	return cloneEnv(env), nil
}

// recordedOutcome turns a persisted dedup record into an API result.
func (o *Orchestrator) recordedOutcome(rec domain.DedupRecord) (*domain.Environment, error) {
	if rec.ErrKind != "" {
		return nil, domain.NewError(rec.ErrKind, "idempotent_replay",
			"replayed failed submission: %s", rec.ErrMsg)
	}
	if e, ok := o.st.Envs[rec.EnvID]; ok {
		return cloneEnv(e), nil
	}
	return nil, domain.NewError(domain.KindNotFound, "replay_missing",
		"recorded environment %s no longer exists", rec.EnvID)
}

// recordFailure persists the failure of an idempotency-keyed submission.
func (o *Orchestrator) recordFailure(key string, envID domain.EnvID, tenant domain.TenantID, err error) {
	if key == "" {
		return
	}
	e := domain.AsError(err)
	rec := domain.DedupRecord{Key: key, EnvID: envID, Tenant: tenant, Done: true, CreatedAt: o.clk.Now()}
	if e != nil {
		rec.ErrKind = e.Kind
		rec.ErrMsg = e.Msg
	} else {
		rec.ErrKind = domain.KindBackend
		rec.ErrMsg = err.Error()
	}
	o.st.Dedup[key] = rec
	o.st.Version++
	_ = o.persistLocked(o.st)
}

// UpdateRequest mutates the mutable parts of a declaration.
type UpdateRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	EnvID          domain.EnvID
	Name           *string
	Resources      *domain.Resources
	TTL            *time.Duration
	DesiredRunning *bool
}

// Update applies a partial declaration update. ID, owner and tenant are
// immutable and cannot be changed through any field. Resource growth is
// settled against quota atomically; insufficient quota rejects the whole
// update (no partial change).
func (o *Orchestrator) Update(ctx context.Context, req UpdateRequest) (*domain.Environment, error) {
	if err := validatePrincipal(req.Principal); err != nil {
		return nil, err
	}
	if req.EnvID == "" {
		return nil, domain.NewError(domain.KindValidation, "env_required", "environment id required")
	}
	if req.Name == nil && req.Resources == nil && req.TTL == nil && req.DesiredRunning == nil {
		return nil, domain.NewError(domain.KindValidation, "empty_update", "at least one mutable field required")
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	key := dedupKey(req.Principal, req.IdempotencyKey)
	if req.IdempotencyKey != "" {
		if rec, ok := o.st.Dedup[key]; ok {
			return o.recordedOutcome(rec)
		}
	}

	env, ok := o.st.Envs[req.EnvID]
	if !ok {
		err := domain.NewError(domain.KindNotFound, "env", "environment %s not found", req.EnvID)
		o.recordFailure(key, req.EnvID, req.Principal.Tenant, err)
		return nil, err
	}
	if err := authorizeWrite(req.Principal, env); err != nil {
		// Do not persist authorization failures into the caller's dedup key
		// under a different identity; but replay for the same caller is fine.
		return nil, err
	}
	if env.Phase.IsTerminal() {
		err := domain.NewError(domain.KindTerminal, "phase_"+string(env.Phase),
			"cannot update environment in terminal phase %s", env.Phase)
		return nil, err
	}

	// Validate candidate values before touching anything.
	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if err := validateName(name); err != nil {
			return nil, err
		}
		if name != env.Spec.Name && o.nameTaken(env.Tenant, name, env.ID) {
			return nil, domain.NewError(domain.KindDuplicate, "name_in_use",
				"tenant %s already has a live environment named %q", env.Tenant, name)
		}
	}
	if req.Resources != nil {
		if err := validateResources(*req.Resources); err != nil {
			return nil, err
		}
	}
	if req.TTL != nil {
		if err := validateTTL(*req.TTL); err != nil {
			return nil, err
		}
	}

	// Atomic quota settlement for the resource delta (growth or shrink).
	if req.Resources != nil {
		delta := req.Resources.Sub(env.Spec.Requested)
		if err := o.qm.CheckDelta(o.st, env, delta); err != nil {
			o.log.Printf("[submit] update rejected by quota env=%s delta=%+v reason=%v", env.ID, delta, err)
			return nil, err
		}
	}

	// Apply changes, keeping reservation aligned with the declared footprint.
	if req.Name != nil {
		env.Spec.Name = strings.TrimSpace(*req.Name)
	}
	if req.Resources != nil {
		env.Spec.Requested = *req.Resources
		env.Reserved = *req.Resources
	}
	if req.TTL != nil {
		env.Spec.TTL = *req.TTL
		if *req.TTL > 0 {
			env.ExpiresAt = o.clk.Now().Add(*req.TTL)
		} else {
			env.ExpiresAt = time.Time{}
		}
	}
	// DesiredRunning is carried by the command payload and applied by the
	// reconciler when the update is the active command, so it composes
	// correctly with concurrently staged start/stop intents.

	c := domain.Command{
		Seq: o.nextSeq(), EnvID: env.ID, Tenant: env.Tenant,
		Type: domain.CmdUpdate, DedupKey: key, EnqueuedAt: o.clk.Now(),
		WantRunning: req.DesiredRunning,
	}
	if err := o.enqueueAndPump(ctx, c); err != nil {
		return cloneEnv(env), err
	}
	o.log.Printf("[submit] update accepted id=%s phase=%s desired=%t reserved=%+v",
		env.ID, env.Phase, env.Spec.DesiredRunning, env.Reserved)
	return cloneEnv(env), nil
}

// lifecycleCmd handles start/stop intents shared validation.
func (o *Orchestrator) lifecycleCmd(ctx context.Context, p domain.Principal, idem string, id domain.EnvID, typ domain.CommandType, want bool) error {
	if err := validatePrincipal(p); err != nil {
		return err
	}
	if id == "" {
		return domain.NewError(domain.KindValidation, "env_required", "environment id required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	key := dedupKey(p, idem)
	if idem != "" {
		if rec, ok := o.st.Dedup[key]; ok {
			_, err := o.recordedOutcome(rec)
			return err
		}
	}
	env, ok := o.st.Envs[id]
	if !ok {
		err := domain.NewError(domain.KindNotFound, "env", "environment %s not found", id)
		o.recordFailure(key, id, p.Tenant, err)
		return err
	}
	if err := authorizeWrite(p, env); err != nil {
		return err
	}
	if env.Phase.IsTerminal() {
		err := domain.NewError(domain.KindTerminal, "phase_"+string(env.Phase),
			"cannot %s a terminal environment", typ)
		return err
	}
	c := domain.Command{
		Seq: o.nextSeq(), EnvID: id, Tenant: env.Tenant,
		Type: typ, DedupKey: key, EnqueuedAt: o.clk.Now(), WantRunning: boolp(want),
	}
	err := o.enqueueAndPump(ctx, c)
	return err
}

func boolp(b bool) *bool { return &b } // StartRequest issues a start intent.
type StartRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	EnvID          domain.EnvID
}

// StopRequest issues a stop intent.
type StopRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	EnvID          domain.EnvID
}

// DeleteRequest issues a delete intent.
type DeleteRequest struct {
	Principal      domain.Principal
	IdempotencyKey string
	EnvID          domain.EnvID
}

// Start requests an environment to be running.
func (o *Orchestrator) Start(ctx context.Context, req StartRequest) error {
	return o.lifecycleCmd(ctx, req.Principal, req.IdempotencyKey, req.EnvID, domain.CmdStart, true)
}

// enqueueLifecycleIntent persists a start/stop/delete intent without pumping,
// so callers (and tests) can stage several conflicting commands and observe
// the deterministic priority resolution on a single convergence pass.
func (o *Orchestrator) enqueueLifecycleIntent(p domain.Principal, id domain.EnvID, typ domain.CommandType, want bool) error {
	if err := validatePrincipal(p); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	env, ok := o.st.Envs[id]
	if !ok {
		return domain.NewError(domain.KindNotFound, "env", "environment %s not found", id)
	}
	if err := authorizeWrite(p, env); err != nil {
		return err
	}
	if env.Phase.IsTerminal() {
		return domain.NewError(domain.KindTerminal, "phase_"+string(env.Phase),
			"cannot %s a terminal environment", typ)
	}
	c := domain.Command{
		Seq: o.nextSeq(), EnvID: id, Tenant: env.Tenant,
		Type: typ, DedupKey: "", EnqueuedAt: o.clk.Now(),
	}
	if typ == domain.CmdStart || typ == domain.CmdStop {
		c.WantRunning = boolp(want)
	}
	o.st.Commands = append(o.st.Commands, c)
	o.st.Version++
	o.log.Printf("[stage] env=%s seq=%d type=%s (no immediate pump)", id, c.Seq, typ)
	return o.persistLocked(o.st)
}

// EnqueueStartIntent stages a start without converging (helper/test use).
func (o *Orchestrator) EnqueueStartIntent(p domain.Principal, id domain.EnvID) error {
	return o.enqueueLifecycleIntent(p, id, domain.CmdStart, true)
}

// EnqueueDeleteIntent stages a delete without converging (helper/test use).
func (o *Orchestrator) EnqueueDeleteIntent(p domain.Principal, id domain.EnvID) error {
	return o.enqueueLifecycleIntent(p, id, domain.CmdDelete, false)
}

// EnqueueStopIntent stages a stop without converging (helper/test use).
func (o *Orchestrator) EnqueueStopIntent(p domain.Principal, id domain.EnvID) error {
	return o.enqueueLifecycleIntent(p, id, domain.CmdStop, false)
}

// Stop requests an environment to be stopped (resources retained, quota held).
func (o *Orchestrator) Stop(ctx context.Context, req StopRequest) error {
	return o.lifecycleCmd(ctx, req.Principal, req.IdempotencyKey, req.EnvID, domain.CmdStop, false)
}

// Delete tears an environment down and releases its quota atomically. Deleting
// an already terminated environment returns not_found; the teardown itself is
// idempotent and resumes safely after a crash.
func (o *Orchestrator) Delete(ctx context.Context, req DeleteRequest) error {
	if err := validatePrincipal(req.Principal); err != nil {
		return err
	}
	if req.EnvID == "" {
		return domain.NewError(domain.KindValidation, "env_required", "environment id required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	key := dedupKey(req.Principal, req.IdempotencyKey)
	if req.IdempotencyKey != "" {
		if rec, ok := o.st.Dedup[key]; ok {
			_, err := o.recordedOutcome(rec)
			return err
		}
	}
	env, ok := o.st.Envs[req.EnvID]
	if !ok {
		err := domain.NewError(domain.KindNotFound, "env", "environment %s not found", req.EnvID)
		o.recordFailure(key, req.EnvID, req.Principal.Tenant, err)
		return err
	}
	if err := authorizeWrite(req.Principal, env); err != nil {
		return err
	}
	if env.Phase == domain.PhaseTerminated {
		return domain.NewError(domain.KindNotFound, "already_deleted",
			"environment %s already deleted", req.EnvID)
	}
	c := domain.Command{
		Seq: o.nextSeq(), EnvID: env.ID, Tenant: env.Tenant,
		Type: domain.CmdDelete, DedupKey: key, EnqueuedAt: o.clk.Now(),
	}
	return o.enqueueAndPump(ctx, c)
}
