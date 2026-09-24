// Package orchestrator is the application facade: it validates declarations,
// authorizes callers, settles quota, persists intent and triggers
// reconciliation. Every decision happens under one state mutex and every
// mutation is fsynced to disk before acceptance is returned, which is what
// makes a process restart recover an identical, consistent result. Duplicate
// submissions carrying the same idempotency key return the recorded outcome
// and produce no additional environment or side effect.
package orchestrator

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/quota"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/reconciler"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/store"
)

// Config parameterizes a new Orchestrator.
type Config struct {
	StorePath string
	Clock     domain.Clock
	Logger    *log.Logger
	Backend   backend.Backend
	// AutoReconcile runs a periodic background convergence pump (transient
	// wobbles self-heal even without an explicit trigger).
	AutoReconcile bool
	// ReconcileInterval sets the background pump period (default 50ms).
	ReconcileInterval time.Duration
}

// Orchestrator is the main entry point of the control plane.
type Orchestrator struct {
	mu  sync.Mutex
	st  *domain.State
	fs  *store.FileStore
	qm  *quota.Manager
	rec *reconciler.Reconciler
	be  backend.Backend
	clk domain.Clock
	log *log.Logger

	cancelBG context.CancelFunc
	wgBG     sync.WaitGroup
}

// New loads persisted state (if any), resumes all interrupted environments at
// startup, and returns a ready orchestrator.
func New(ctx context.Context, cfg Config) (*Orchestrator, error) {
	if cfg.Clock == nil {
		cfg.Clock = domain.SystemClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stdout, "[orch] ", log.LstdFlags)
	}
	if cfg.Backend == nil {
		cfg.Backend = backend.NewFakeBackend()
	}
	fs := store.NewFileStore(cfg.StorePath)
	st, err := fs.Load()
	if err != nil {
		return nil, err
	}
	if st.Dedup == nil {
		st.Dedup = map[string]domain.DedupRecord{}
	}
	if st.TenantLimits == nil {
		st.TenantLimits = map[domain.TenantID]domain.Resources{}
	}
	o := &Orchestrator{
		st:  st,
		fs:  fs,
		qm:  quota.NewManager(),
		rec: reconciler.New(cfg.Backend, cfg.Clock, cfg.Logger),
		be:  cfg.Backend,
		clk: cfg.Clock,
		log: cfg.Logger,
	}
	o.log.Printf("[boot] loaded state version=%d envs=%d pending_commands=%d from %s",
		st.Version, len(st.Envs), len(st.Commands), cfg.StorePath)

	// Startup recovery: resume every in-flight / teardown environment from
	// the last persisted step. This is the same idempotent Pump used online.
	if _, err := o.rec.Pump(ctx, st, o.persistLocked); err != nil {
		return nil, err
	}
	if err := o.persistLocked(st); err != nil {
		return nil, err
	}

	if cfg.AutoReconcile {
		bgctx, cancel := context.WithCancel(context.Background())
		o.cancelBG = cancel
		interval := cfg.ReconcileInterval
		if interval <= 0 {
			interval = 50 * time.Millisecond
		}
		o.wgBG.Add(1)
		go o.backgroundPump(bgctx, interval)
	}
	return o, nil
}

// Close stops the background pump (if any). Persistent state is already safe on
// disk after every accepted mutation, so Close needs no final flush.
func (o *Orchestrator) Close() {
	if o.cancelBG != nil {
		o.cancelBG()
		o.wgBG.Wait()
	}
}

func (o *Orchestrator) backgroundPump(ctx context.Context, interval time.Duration) {
	defer o.wgBG.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = o.TriggerReconcile(ctx)
		}
	}
}

// persistLocked stores the current state. Caller must hold o.mu (the pump runs
// inside the same critical section, so reconciliation + persistence are atomic
// relative to API submissions).
func (o *Orchestrator) persistLocked(st *domain.State) error {
	return o.fs.Save(st)
}

// Snapshot returns a deep-enough copy of one environment for read-only callers.
func cloneEnv(e *domain.Environment) *domain.Environment {
	c := *e
	if e.BackingIDs != nil {
		c.BackingIDs = append([]string(nil), e.BackingIDs...)
	}
	return &c
}

// nextSeq returns the next monotonic command sequence.
func (o *Orchestrator) nextSeq() int64 {
	o.st.LastSeq++
	return o.st.LastSeq
}

// dedupKey namespaces a client key by tenant so keys never collide across
// tenants and are globally unique in the persisted record.
func dedupKey(p domain.Principal, clientKey string) string {
	return string(p.Tenant) + "|" + clientKey
}

// enqueue appends a command, persists intent first (write-ahead), then pumps.
func (o *Orchestrator) enqueueAndPump(ctx context.Context, c domain.Command) error {
	// Reserve the idempotency key before any side effect so a retry arriving
	// while the first submission is still converging (or after a crash
	// mid-creation) binds to the same environment instead of making another.
	if c.DedupKey != "" {
		if _, exists := o.st.Dedup[c.DedupKey]; !exists {
			o.st.Dedup[c.DedupKey] = domain.DedupRecord{
				Key: c.DedupKey, EnvID: c.EnvID, Tenant: c.Tenant,
				Done: false, CreatedAt: o.clk.Now(),
			}
		}
	}
	o.st.Commands = append(o.st.Commands, c)
	o.st.Version++
	o.log.Printf("[submit] env=%s tenant=%s seq=%d type=%s key=%q",
		c.EnvID, c.Tenant, c.Seq, c.Type, c.DedupKey)
	if err := o.persistLocked(o.st); err != nil {
		return err
	}
	_, err := o.rec.Pump(ctx, o.st, o.persistLocked)
	return err
}

// Get returns a snapshot of one environment the principal may see.
func (o *Orchestrator) Get(p domain.Principal, id domain.EnvID) (*domain.Environment, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	e, ok := o.st.Envs[id]
	if !ok {
		return nil, domain.NewError(domain.KindNotFound, "env", "environment %s not found", id)
	}
	if err := authorizeRead(p, e); err != nil {
		return nil, err
	}
	return cloneEnv(e), nil
}

// List returns all environments visible to the principal.
func (o *Orchestrator) List(p domain.Principal) ([]*domain.Environment, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if p.Tenant == "" {
		return nil, domain.NewError(domain.KindValidation, "principal", "principal tenant required")
	}
	out := make([]*domain.Environment, 0, len(o.st.Envs))
	for _, e := range o.st.Envs {
		if p.Role == domain.RoleAdmin || e.Tenant == p.Tenant {
			out = append(out, cloneEnv(e))
		}
	}
	return out, nil
}

// TriggerReconcile runs one convergence pump synchronously.
func (o *Orchestrator) TriggerReconcile(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	n, err := o.rec.Pump(ctx, o.st, o.persistLocked)
	if err != nil {
		return err
	}
	if n > 0 {
		o.log.Printf("[trigger] pump applied %d commands", n)
	}
	return nil
}

// SetTenantLimit configures the quota limit for a tenant (admin only).
func (o *Orchestrator) SetTenantLimit(p domain.Principal, t domain.TenantID, lim domain.Resources) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if p.Role != domain.RoleAdmin {
		return domain.NewError(domain.KindForbidden, "admin_only", "only admin may set tenant limits")
	}
	if err := o.qm.CheckShrinkTenant(o.st, t, lim); err != nil {
		return err
	}
	o.st.TenantLimits[t] = lim
	o.st.Version++
	o.log.Printf("[quota] tenant=%s limit set to %+v", t, lim)
	return o.persistLocked(o.st)
}

// SetClusterCapacity configures global capacity (admin only).
func (o *Orchestrator) SetClusterCapacity(p domain.Principal, lim domain.Resources) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if p.Role != domain.RoleAdmin {
		return domain.NewError(domain.KindForbidden, "admin_only", "only admin may set cluster capacity")
	}
	if err := o.qm.CheckShrinkCapacity(o.st, lim); err != nil {
		return err
	}
	o.st.ClusterCapacity = lim
	o.st.Version++
	o.log.Printf("[quota] cluster capacity set to %+v (used %+v)", lim, o.qm.ClusterUsed(o.st))
	return o.persistLocked(o.st)
}

// TenantUsage reports current usage for a tenant (tenant members or admin).
func (o *Orchestrator) TenantUsage(p domain.Principal, t domain.TenantID) (domain.Resources, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if p.Role != domain.RoleAdmin && p.Tenant != t {
		return domain.Resources{}, domain.NewError(domain.KindForbidden, "cross_tenant",
			"principal of %s cannot read usage of %s", p.Tenant, t)
	}
	return o.qm.Usage(o.st, t), nil
}
