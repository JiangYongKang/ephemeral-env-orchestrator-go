package model

import "time"

// NewState returns the initial empty control-plane document.
func NewState() *State {
	return &State{
		Version:    1,
		Envs:       map[string]*Environment{},
		Quotas:     map[string]*TenantQuota{},
		Operations: []*Operation{},
	}
}

// Mul scales a resource vector by n (n>=0).
func (r Resources) Mul(n int) Resources {
	if n < 0 {
		n = 0
	}
	return Resources{CPUMilli: r.CPUMilli * n, MemoryMB: r.MemoryMB * n, StorageGB: r.StorageGB * n}
}

// Add sums two resource vectors.
func (r Resources) Add(o Resources) Resources {
	return Resources{
		CPUMilli:  r.CPUMilli + o.CPUMilli,
		MemoryMB:  r.MemoryMB + o.MemoryMB,
		StorageGB: r.StorageGB + o.StorageGB,
	}
}

// Requested returns the total resource vector declared by the spec.
func (s EnvSpec) Requested() Resources { return s.Resources.Mul(s.Replicas) }

// Allocated sums the resources of an environment's live allocations.
func (e *Environment) Allocated() Resources {
	var sum Resources
	for i := range e.Allocations {
		sum = sum.Add(e.Allocations[i].Resources)
	}
	return sum
}

// TenantUsage derives a tenant's currently materialized resource usage from
// allocations. Deriving usage (rather than trusting counters) means a crash
// can never drift quota accounting.
func TenantUsage(st *State, tenantID string) Resources {
	var sum Resources
	for _, e := range st.Envs {
		if e.Spec.TenantID != tenantID || e.Deleted {
			continue
		}
		sum = sum.Add(e.Allocated())
	}
	return sum
}

// ClusterUsage derives total materialized resources across all tenants.
func ClusterUsage(st *State) Resources {
	var sum Resources
	for _, e := range st.Envs {
		if e.Deleted {
			continue
		}
		sum = sum.Add(e.Allocated())
	}
	return sum
}

// TenantReserved derives the resources a tenant has declared and is entitled
// to converge toward. Environments pending creation count their full spec so
// concurrent creates can never over-commit; Stopped/Expired environments
// reserve nothing (their quota has been released and is re-checked on start).
func TenantReserved(st *State, tenantID string) Resources {
	var sum Resources
	for _, e := range st.Envs {
		if e.Spec.TenantID != tenantID || e.Deleted {
			continue
		}
		if e.Desired == DesiredRunning {
			sum = sum.Add(e.Spec.Requested())
		}
	}
	return sum
}

// ClusterReserved is the cluster-wide analog of TenantReserved.
func ClusterReserved(st *State) Resources {
	var sum Resources
	for _, e := range st.Envs {
		if e.Deleted || e.Desired != DesiredRunning {
			continue
		}
		sum = sum.Add(e.Spec.Requested())
	}
	return sum
}

// IsExpired reports whether an env has passed its TTL at time now.
func (e *Environment) IsExpired(now time.Time) bool {
	if e.Desired == DesiredDeleted || e.Deleted {
		return false
	}
	return !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt)
}

// IsTerminalTombstone reports whether the record is a fully removed tombstone.
func (e *Environment) IsTerminalTombstone() bool {
	return e.Deleted && e.Phase == PhaseDeleting && len(e.Allocations) == 0
}
