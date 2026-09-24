// Package quota computes tenant quota and global cluster-capacity settlement.
// It performs no I/O and holds no locks of its own: the orchestrator invokes
// it while holding the single state mutex, which keeps tenant reservation
// accounting and global capacity settlement atomic against concurrent
// applications, capacity changes and deletions.
//
// Reservations are derived from each live environment's Reserved field, so
// there is no second counter that could drift after a restart: releasing a
// reservation is just clearing the environment's Reserved value inside the
// same persisted transaction as its phase transition.
package quota

import (
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// Manager evaluates quota against a domain.State.
type Manager struct{}

// NewManager constructs a Manager.
func NewManager() *Manager { return &Manager{} }

// isLive reserves quota: a terminating/terminated/failed environment holds no
// reservation. Stopped environments still hold theirs (footprint retained).
func isLive(p domain.Phase) bool {
	switch p {
	case domain.PhaseProvisioning, domain.PhaseRunning,
		domain.PhaseStopping, domain.PhaseStopped, domain.PhaseStarting:
		return true
	default:
		return false
	}
}

// tenantUsage sums Reserved over the tenant's live environments.
func tenantUsage(st *domain.State, t domain.TenantID) domain.Resources {
	var u domain.Resources
	for _, e := range st.Envs {
		if e.Tenant != t || !isLive(e.Phase) {
			continue
		}
		u = u.Add(e.Reserved)
	}
	return u
}

// Usage returns the resources currently reserved by tenant.
func (m *Manager) Usage(st *domain.State, t domain.TenantID) domain.Resources {
	return tenantUsage(st, t)
}

// ClusterUsed returns resources reserved across all tenants.
func (m *Manager) ClusterUsed(st *domain.State) domain.Resources {
	var u domain.Resources
	for _, e := range st.Envs {
		if isLive(e.Phase) {
			u = u.Add(e.Reserved)
		}
	}
	return u
}

// unlimited means "no limit configured" on a dimension (zero value).
func unlimited(r domain.Resources) bool {
	return r.CPU == 0 && r.Memory == 0 && r.Storage == 0
}

// CheckDelta validates that changing the given environment's reservation by
// delta (positive for growth, negative for shrink) keeps the tenant within its
// per-tenant limit and the whole cluster within global capacity.
//
// distinct error codes:
//   - tenant_quota : the tenant's own limit would be exceeded
//   - cluster_capacity : global capacity would be exceeded
func (m *Manager) CheckDelta(st *domain.State, env *domain.Environment, delta domain.Resources) error {
	t := env.Tenant
	tenantUsed := tenantUsage(st, t)
	// This env's own current reservation is already included in tenantUsed;
	// the candidate total is used - current + (current+delta) = used + delta.
	candidateTenant := tenantUsed.Add(delta)
	if lim, ok := st.TenantLimits[t]; ok && !unlimited(lim) {
		if !candidateTenant.Fits(lim) {
			return domain.NewError(domain.KindQuota, "tenant_quota",
				"tenant %s quota exceeded: requested delta %+v would reserve %+v > limit %+v (used %+v)",
				t, delta, candidateTenant, lim, tenantUsed)
		}
	}
	clusterUsed := m.ClusterUsed(st)
	candidateCluster := clusterUsed.Add(delta)
	if !unlimited(st.ClusterCapacity) && !candidateCluster.Fits(st.ClusterCapacity) {
		return domain.NewError(domain.KindQuota, "cluster_capacity",
			"global capacity exceeded: delta %+v would reserve %+v > capacity %+v (used %+v)",
			delta, candidateCluster, st.ClusterCapacity, clusterUsed)
	}
	return nil
}

// CheckNew validates reservation of res for a brand new environment of tenant.
func (m *Manager) CheckNew(st *domain.State, t domain.TenantID, res domain.Resources) error {
	if lim, ok := st.TenantLimits[t]; ok && !unlimited(lim) {
		if !res.Fits(lim) {
			return domain.NewError(domain.KindQuota, "tenant_quota",
				"tenant %s quota exceeded: request %+v > limit %+v", t, res, lim)
		}
		if used := tenantUsage(st, t); !used.Add(res).Fits(lim) {
			return domain.NewError(domain.KindQuota, "tenant_quota",
				"tenant %s quota exceeded: used %+v + request %+v > limit %+v", t, used, res, lim)
		}
	}
	clusterUsed := m.ClusterUsed(st)
	if !unlimited(st.ClusterCapacity) && !clusterUsed.Add(res).Fits(st.ClusterCapacity) {
		return domain.NewError(domain.KindQuota, "cluster_capacity",
			"global capacity exceeded: used %+v + request %+v > capacity %+v",
			clusterUsed, res, st.ClusterCapacity)
	}
	return nil
}

// ValidateLimit rejects a malformed (negative) limit.
func ValidateLimit(r domain.Resources) error {
	if r.CPU < 0 || r.Memory < 0 || r.Storage < 0 {
		return domain.NewError(domain.KindValidation, "negative_limit",
			"resource limit must not be negative: %+v", r)
	}
	return nil
}

// CheckShrinkCapacity reports whether setting global capacity to next stays
// valid for currently reserved resources (capacity may never be shrunk below
// usage; deletion/release must happen first).
func (m *Manager) CheckShrinkCapacity(st *domain.State, next domain.Resources) error {
	if err := ValidateLimit(next); err != nil {
		return err
	}
	if unlimited(next) {
		return nil
	}
	used := m.ClusterUsed(st)
	if !used.Fits(next) {
		return domain.NewError(domain.KindQuota, "capacity_in_use",
			"cannot shrink capacity to %+v: %+v currently reserved", next, used)
	}
	return nil
}

// CheckShrinkTenant reports whether lowering tenant t's limit to next stays
// valid for that tenant's current reservations.
func (m *Manager) CheckShrinkTenant(st *domain.State, t domain.TenantID, next domain.Resources) error {
	if err := ValidateLimit(next); err != nil {
		return err
	}
	if unlimited(next) {
		return nil
	}
	used := tenantUsage(st, t)
	if !used.Fits(next) {
		return domain.NewError(domain.KindQuota, "tenant_limit_in_use",
			"cannot lower tenant %s limit to %+v: %+v currently reserved", t, next, used)
	}
	return nil
}
