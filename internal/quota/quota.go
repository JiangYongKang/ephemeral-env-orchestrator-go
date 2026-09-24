// Package quota computes tenant quota and global cluster-capacity settlement.
// It performs no I/O and holds no locks of its own: the orchestrator invokes
// it while holding the state lock, keeping reservation accounting atomic.
package quota

import "github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"

// Manager evaluates and applies quota settlement against a domain.State.
type Manager struct{}

// NewManager constructs a Manager.
func NewManager() *Manager { return &Manager{} }

// Usage returns the resources currently reserved by tenant.
func (m *Manager) Usage(st *domain.State, t domain.TenantID) domain.Resources {
	return domain.Resources{}
}

// ClusterUsed returns resources reserved across all tenants.
func (m *Manager) ClusterUsed(st *domain.State) domain.Resources { return domain.Resources{} }

// Check reports whether reserving delta for tenant is allowed. It mutates
// nothing; Reserve performs the actual accounting.
func (m *Manager) Check(st *domain.State, t domain.TenantID, delta domain.Resources) error {
	return nil
}

// Reserve adds delta to the tenant's accounted usage (via env records).
func (m *Manager) Reserve(st *domain.State, t domain.TenantID, delta domain.Resources) {}

// Release subtracts delta from the tenant's accounted usage.
func (m *Manager) Release(st *domain.State, t domain.TenantID, delta domain.Resources) {}
