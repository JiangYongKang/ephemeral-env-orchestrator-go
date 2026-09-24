// Package model defines the declarative environment model, state machine,
// and the persistent local control-plane state.
package model

import "time"

// Resources is a resource vector consumed by an environment.
type Resources struct {
	CPUMilli  int `json:"cpuMilli"`
	MemoryMB  int `json:"memoryMB"`
	StorageGB int `json:"storageGB"`
}

// Phase is the observed lifecycle phase of an environment.
type Phase string

const (
	PhasePending  Phase = "Pending"
	PhaseStarting Phase = "Starting"
	PhaseRunning  Phase = "Running"
	PhaseStopping Phase = "Stopping"
	PhaseStopped  Phase = "Stopped"
	PhaseExpired  Phase = "Expired"
	PhaseFailed   Phase = "Failed"
	PhaseDeleting Phase = "Deleting"
)

// Desired is the declarative desired state of an environment.
type Desired string

const (
	DesiredRunning Desired = "Running"
	DesiredStopped Desired = "Stopped"
	DesiredDeleted Desired = "Deleted"
)

// EnvSpec is the immutable-on-create / partially-mutable-on-update declaration.
type EnvSpec struct {
	Name       string    `json:"name"`
	Owner      string    `json:"owner"`
	TenantID   string    `json:"tenantId"`
	Image      string    `json:"image"`
	Replicas   int       `json:"replicas"`
	Resources  Resources `json:"resources"`
	TTLSeconds int       `json:"ttlSeconds"`
	CreatedAt  time.Time `json:"createdAt"`
}

// Allocation is one materialized (simulated) resource instance.
type Allocation struct {
	ID        string    `json:"id"`
	EnvID     string    `json:"envId"`
	TenantID  string    `json:"tenantId"`
	Image     string    `json:"image"`
	Resources Resources `json:"resources"`
	StartedAt time.Time `json:"startedAt"`
}

// Environment is the persisted per-environment record (declared + observed).
type Environment struct {
	ID          string       `json:"id"`
	Spec        EnvSpec      `json:"spec"`
	Desired     Desired      `json:"desired"`
	Phase       Phase        `json:"phase"`
	Allocations []Allocation `json:"allocations"`
	FailReason  string       `json:"failReason,omitempty"`
	Attempts    int          `json:"attempts"`
	Generation  int64        `json:"generation"`
	CreatedAt   time.Time    `json:"createdAt"`
	UpdatedAt   time.Time    `json:"updatedAt"`
	ExpiresAt   time.Time    `json:"expiresAt"`
	Deleted     bool         `json:"deleted"`
}

// TenantQuota declares per-tenant resource limits.
type TenantQuota struct {
	TenantID  string    `json:"tenantId"`
	CPUMilli  int       `json:"cpuMilli"`
	MemoryMB  int       `json:"memoryMB"`
	StorageGB int       `json:"storageGB"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ClusterCapacity is the global shared capacity.
type ClusterCapacity struct {
	CPUMilli  int       `json:"cpuMilli"`
	MemoryMB  int       `json:"memoryMB"`
	StorageGB int       `json:"storageGB"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// OpKind enumerates serialized mutating operations per environment.
type OpKind string

const (
	OpCreate OpKind = "Create"
	OpUpdate OpKind = "Update"
	OpStart  OpKind = "Start"
	OpStop   OpKind = "Stop"
	OpDelete OpKind = "Delete"
)

// Operation is a queued intent with deterministic conflict priority.
// Lower Priority value wins.
type Operation struct {
	ID        string    `json:"id"`
	EnvID     string    `json:"envId"`
	TenantID  string    `json:"tenantId"`
	Actor     string    `json:"actor"`
	Kind      OpKind    `json:"kind"`
	Priority  int       `json:"priority"`
	Seq       int64     `json:"seq"`
	NewSpec   *EnvSpec  `json:"newSpec,omitempty"`
	Done      bool      `json:"done"`
	Rejected  bool      `json:"rejected"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

// State is the single, atomically persisted control-plane document.
type State struct {
	Version    int                     `json:"version"`
	Envs       map[string]*Environment `json:"envs"`
	Quotas     map[string]*TenantQuota `json:"quota"`
	Capacity   ClusterCapacity         `json:"capacity"`
	Operations []*Operation            `json:"operations"`
	Seq        int64                   `json:"seq"`
	UpdatedAt  time.Time               `json:"updatedAt"`
}
