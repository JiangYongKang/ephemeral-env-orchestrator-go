// Package domain contains the core domain model of the ephemeral environment
// orchestrator: environments, commands, quota state and classification of errors.
//
// The model is intentionally free of I/O so it can be unit-tested in isolation
// and serialized verbatim to the local persistent state file.
package domain

import "time"

// TenantID identifies a tenant (organization). All resources are strictly
// scoped to a tenant.
type TenantID string

// EnvID identifies an environment. It is globally unique and immutable.
type EnvID string

// UserID identifies an authenticated principal.
type UserID string

// Role of a principal. Admin may act across tenants; user is limited to the
// tenant it belongs to and to environments it owns.
type Role string

const (
	RoleUser  Role = "user"
	RoleAdmin Role = "admin"
)

// Principal is the authenticated caller of an API operation.
type Principal struct {
	User   UserID
	Tenant TenantID
	Role   Role
}

// Resources describes a resource footprint. All fields must be >= 0.
type Resources struct {
	CPU     int `json:"cpu"`
	Memory  int `json:"memory"`
	Storage int `json:"storage"`
}

// Add returns r+o.
func (r Resources) Add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory, Storage: r.Storage + o.Storage}
}

// Sub returns r-o; callers must ensure no field goes negative.
func (r Resources) Sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory, Storage: r.Storage - o.Storage}
}

// Fits reports whether r fits inside limit on every dimension.
func (r Resources) Fits(limit Resources) bool {
	return r.CPU <= limit.CPU && r.Memory <= limit.Memory && r.Storage <= limit.Storage
}

// Phase of an environment, see package docs for the state machine.
type Phase string

const (
	// PhaseProvisioning: create accepted, backing resources being created.
	PhaseProvisioning Phase = "provisioning"
	// PhaseRunning: backing resources exist and are started.
	PhaseRunning Phase = "running"
	// PhaseStopping: transition running -> stopped in progress.
	PhaseStopping Phase = "stopping"
	// PhaseStopped: resources exist but are stopped; quota still held.
	PhaseStopped Phase = "stopped"
	// PhaseStarting: transition stopped -> running in progress.
	PhaseStarting Phase = "starting"
	// PhaseExpiring: expired, resources being torn down.
	PhaseExpiring Phase = "expiring"
	// PhaseDeleting: explicit delete, resources being torn down.
	PhaseDeleting Phase = "deleting"
	// PhaseTerminated: terminal tombstone (TTL expiry or delete complete).
	PhaseTerminated Phase = "terminated"
	// PhaseFailed: unrecoverable failure; reason explains the cause.
	PhaseFailed Phase = "failed"
)

// IsTerminal reports whether the phase is terminal.
func (p Phase) IsTerminal() bool { return p == PhaseTerminated || p == PhaseFailed }

// Spec is the declarative, user-provided part of an environment.
type Spec struct {
	// Name is mutable but unique within a tenant.
	Name string `json:"name"`
	// Owner is the creating user; immutable afterwards.
	Owner UserID `json:"owner"`
	// Requested is the resource footprint; mutable subject to quota.
	Requested Resources `json:"requested"`
	// TTL is the validity duration from creation/update. Zero disables expiry.
	TTL time.Duration `json:"ttl"`
	// DesiredRunning is the user intent: true = run, false = stopped.
	DesiredRunning bool `json:"desired_running"`
}

// CommandType enumerates the mutating intents applied to an environment.
type CommandType string

const (
	CmdCreate CommandType = "create"
	CmdUpdate CommandType = "update"
	CmdStart  CommandType = "start"
	CmdStop   CommandType = "stop"
	CmdDelete CommandType = "delete"
	// CmdExpire is injected by the system when an environment passes its TTL.
	CmdExpire CommandType = "expire"
	// CmdRecover is injected by the reconciler to resume interrupted work.
	CmdRecover CommandType = "recover"
)

// Priority defines deterministic conflict resolution: smaller wins.
// Delete/expire (teardown) beat mutating lifecycle ops, which beat start/stop.
func (c CommandType) Priority() int {
	switch c {
	case CmdDelete, CmdExpire:
		return 0
	case CmdRecover:
		return 1
	case CmdCreate, CmdUpdate, CmdStop:
		return 2
	case CmdStart:
		return 3
	default:
		return 9
	}
}

// Command is a unit of intent. Commands are persisted before any side effect
// so an interrupted process can resume; the dedup key makes retries idempotent.
type Command struct {
	Seq        int64       `json:"seq"`
	EnvID      EnvID       `json:"env_id"`
	Tenant     TenantID    `json:"tenant"`
	Type       CommandType `json:"type"`
	DedupKey   string      `json:"dedup_key"`
	SpecPatch  *Spec       `json:"spec_patch,omitempty"`
	EnqueuedAt time.Time   `json:"enqueued_at"`
}

// Environment is the full persisted record: spec + observed status.
type Environment struct {
	ID     EnvID    `json:"id"`
	Tenant TenantID `json:"tenant"`
	Spec   Spec     `json:"spec"`
	Phase  Phase    `json:"phase"`
	// Resources currently reserved against quota (0 after termination).
	Reserved Resources `json:"reserved"`
	// Backing IDs created in the resource backend, for teardown/resume.
	BackingIDs []string `json:"backing_ids,omitempty"`
	// Running reflects whether backing resources are currently started.
	Running   bool      `json:"running"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	// FailureReason is set when Phase == failed; empty otherwise.
	FailureReason string `json:"failure_reason,omitempty"`
	// LastSeq is the highest command seq already fully applied.
	LastSeq int64 `json:"last_seq"`
}

// DedupRecord remembers the outcome of an idempotency-keyed submission so a
// retry returns the same outcome and performs no extra side effects.
type DedupRecord struct {
	Key       string     `json:"key"`
	EnvID     EnvID      `json:"env_id"`
	Tenant    TenantID   `json:"tenant"`
	Done      bool       `json:"done"`
	ErrKind   ErrorKind  `json:"err_kind,omitempty"`
	ErrMsg    string     `json:"err_msg,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

// State is the complete persistent control-plane snapshot.
type State struct {
	// Envs keyed by environment id.
	Envs map[EnvID]*Environment `json:"envs"`
	// Pending commands waiting to be reconciled, in global seq order.
	Commands []Command `json:"commands"`
	// Dedup maps an idempotency key to its recorded outcome.
	Dedup map[string]DedupRecord `json:"dedup"`
	// TenantLimits: configured quota per tenant (zero = unlimited default).
	TenantLimits map[TenantID]Resources `json:"tenant_limits"`
	// ClusterCapacity is the global hard capacity shared by all tenants.
	ClusterCapacity Resources `json:"cluster_capacity"`
	// LastSeq is the monotonic command sequence counter.
	LastSeq int64 `json:"last_seq"`
	// Version is incremented on every persisted mutation (crash consistency aid).
	Version int64 `json:"version"`
}
