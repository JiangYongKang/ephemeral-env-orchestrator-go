// Package quota computes multi-tenant quota and cluster-capacity settlement.
//
// Reservation model: an environment reserves its declared spec whenever its
// desired state is Running. Reservations are derived from the persisted
// state (not increment/decrement counters), so a crash or partial failure
// can never desynchronize accounting. Actual usage never exceeds reservation
// because convergence only materializes allocations within the spec.
//
// All checks run while the orchestrator holds the single state lock, hence
// the check-then-reserve sequence is atomic relative to concurrent requests.
package quota

import (
	"fmt"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// Checker evaluates whether an additionally requested resource vector fits
// both the tenant quota and the shared cluster capacity.
type Checker struct{}

// NewChecker creates a quota checker.
func NewChecker() *Checker { return &Checker{} }

// Fits reports whether req can be added on top of current reservations for
// tenantID. When false, reason names the violated limit and dimension.
func (c *Checker) Fits(st *model.State, tenantID string, req model.Resources) (bool, string) {
	if req.CPUMilli < 0 || req.MemoryMB < 0 || req.StorageGB < 0 {
		return false, "requested resources must be non-negative"
	}

	q, ok := st.Quotas[tenantID]
	if !ok {
		return false, fmt.Sprintf("tenant %q has no quota configured", tenantID)
	}
	reserved := model.TenantReserved(st, tenantID)
	if reserved.CPUMilli+req.CPUMilli > q.CPUMilli {
		return false, fmt.Sprintf("tenant %q cpu quota exceeded: reserved=%d + requested=%d > limit=%d milli",
			tenantID, reserved.CPUMilli, req.CPUMilli, q.CPUMilli)
	}
	if reserved.MemoryMB+req.MemoryMB > q.MemoryMB {
		return false, fmt.Sprintf("tenant %q memory quota exceeded: reserved=%d + requested=%d > limit=%d MB",
			tenantID, reserved.MemoryMB, req.MemoryMB, q.MemoryMB)
	}
	if reserved.StorageGB+req.StorageGB > q.StorageGB {
		return false, fmt.Sprintf("tenant %q storage quota exceeded: reserved=%d + requested=%d > limit=%d GB",
			tenantID, reserved.StorageGB, req.StorageGB, q.StorageGB)
	}

	cap := st.Capacity
	cluster := model.ClusterReserved(st)
	if cluster.CPUMilli+req.CPUMilli > cap.CPUMilli {
		return false, fmt.Sprintf("cluster cpu capacity exceeded: reserved=%d + requested=%d > capacity=%d milli",
			cluster.CPUMilli, req.CPUMilli, cap.CPUMilli)
	}
	if cluster.MemoryMB+req.MemoryMB > cap.MemoryMB {
		return false, fmt.Sprintf("cluster memory capacity exceeded: reserved=%d + requested=%d > capacity=%d MB",
			cluster.MemoryMB, req.MemoryMB, cap.MemoryMB)
	}
	if cluster.StorageGB+req.StorageGB > cap.StorageGB {
		return false, fmt.Sprintf("cluster storage capacity exceeded: reserved=%d + requested=%d > capacity=%d GB",
			cluster.StorageGB, req.StorageGB, cap.StorageGB)
	}
	return true, ""
}

// CheckChange validates moving one environment's running reservation from
// oldRes to newRes against other tenants' reservations. The environment's
// own current reservation is excluded, then newRes is tested in full, so
// shrinking always passes and growth is checked precisely.
func (c *Checker) CheckChange(st *model.State, tenantID string, oldRes, newRes model.Resources) (bool, string) {
	return c.checkExcluding(st, tenantID, oldRes, newRes)
}

// checkExcluding computes tenant/cluster reservations ignoring oldRes (the
// moving env's current reservation) and then admitting newRes.
func (c *Checker) checkExcluding(st *model.State, tenantID string, oldRes, newRes model.Resources) (bool, string) {
	if newRes.CPUMilli < 0 || newRes.MemoryMB < 0 || newRes.StorageGB < 0 {
		return false, "requested resources must be non-negative"
	}
	q, ok := st.Quotas[tenantID]
	if !ok {
		return false, fmt.Sprintf("tenant %q has no quota configured", tenantID)
	}
	tRes := model.TenantReserved(st, tenantID)
	free := model.Resources{
		CPUMilli:  q.CPUMilli - (tRes.CPUMilli - oldRes.CPUMilli),
		MemoryMB:  q.MemoryMB - (tRes.MemoryMB - oldRes.MemoryMB),
		StorageGB: q.StorageGB - (tRes.StorageGB - oldRes.StorageGB),
	}
	if newRes.CPUMilli > free.CPUMilli {
		return false, fmt.Sprintf("tenant %q cpu quota exceeded: new=%d > available=%d milli (limit=%d)",
			tenantID, newRes.CPUMilli, free.CPUMilli, q.CPUMilli)
	}
	if newRes.MemoryMB > free.MemoryMB {
		return false, fmt.Sprintf("tenant %q memory quota exceeded: new=%d > available=%d MB (limit=%d)",
			tenantID, newRes.MemoryMB, free.MemoryMB, q.MemoryMB)
	}
	if newRes.StorageGB > free.StorageGB {
		return false, fmt.Sprintf("tenant %q storage quota exceeded: new=%d > available=%d GB (limit=%d)",
			tenantID, newRes.StorageGB, free.StorageGB, q.StorageGB)
	}

	cap := st.Capacity
	cRes := model.ClusterReserved(st)
	cFree := model.Resources{
		CPUMilli:  cap.CPUMilli - (cRes.CPUMilli - oldRes.CPUMilli),
		MemoryMB:  cap.MemoryMB - (cRes.MemoryMB - oldRes.MemoryMB),
		StorageGB: cap.StorageGB - (cRes.StorageGB - oldRes.StorageGB),
	}
	if newRes.CPUMilli > cFree.CPUMilli {
		return false, fmt.Sprintf("cluster cpu capacity exceeded: new=%d > available=%d milli",
			newRes.CPUMilli, cFree.CPUMilli)
	}
	if newRes.MemoryMB > cFree.MemoryMB {
		return false, fmt.Sprintf("cluster memory capacity exceeded: new=%d > available=%d MB",
			newRes.MemoryMB, cFree.MemoryMB)
	}
	if newRes.StorageGB > cFree.StorageGB {
		return false, fmt.Sprintf("cluster storage capacity exceeded: new=%d > available=%d GB",
			newRes.StorageGB, cFree.StorageGB)
	}
	return true, ""
}
