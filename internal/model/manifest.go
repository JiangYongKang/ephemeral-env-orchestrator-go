package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"time"
)

// ManifestAction is the per-item intent of a manifest entry.
type ManifestAction string

const (
	// ManifestUpsert declares the full desired spec of an environment:
	// created when absent, converged when present.
	ManifestUpsert ManifestAction = "Upsert"
	// ManifestDelete declares the environment must be absent.
	ManifestDelete ManifestAction = "Delete"
)

// ManifestItem is one entry of a manifest. For Upsert the item is a FULL
// declaration: image/replicas/resources/ttlSeconds/desired all default to
// their zero values only when the zero value is itself meaningful
// (desired defaults to Running, action defaults to Upsert).
type ManifestItem struct {
	Name       string         `json:"name"`
	Action     ManifestAction `json:"action,omitempty"`
	Image      string         `json:"image,omitempty"`
	Replicas   int            `json:"replicas,omitempty"`
	Resources  Resources      `json:"resources,omitempty"`
	TTLSeconds int            `json:"ttlSeconds,omitempty"`
	Desired    Desired        `json:"desired,omitempty"`
}

// Manifest is a batch declaration of a whole set of environments, applied
// and settled as one unit.
type Manifest struct {
	Items []ManifestItem `json:"items"`
}

// Normalized returns the item with defaults applied (Upsert / Running).
func (it ManifestItem) Normalized() ManifestItem {
	if it.Action == "" {
		it.Action = ManifestUpsert
	}
	if it.Action == ManifestUpsert && it.Desired == "" {
		it.Desired = DesiredRunning
	}
	return it
}

// ID derives a deterministic content address for the manifest within a
// tenant: identical manifests submitted repeatedly (or concurrently) map to
// the same batch, which is what makes re-application idempotent. Items are
// sorted canonically before hashing so submission order of *independent*
// items does not matter; conflicting same-name items keep their relative
// order (stable sort) because order decides which one wins.
func (m Manifest) ID(tenantID string) string {
	items := make([]ManifestItem, len(m.Items))
	for i, it := range m.Items {
		items[i] = it.Normalized()
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	buf, _ := json.Marshal(items)
	sum := sha256.Sum256(append([]byte(tenantID+"\x00"), buf...))
	return "mf-" + hex.EncodeToString(sum[:8])
}

// BatchItemStatus is the terminal-or-pending state of one applied item.
type BatchItemStatus string

const (
	BatchItemPending BatchItemStatus = "Pending"
	BatchItemApplied BatchItemStatus = "Applied"
	BatchItemFailed  BatchItemStatus = "Failed"
)

// BatchItem records the persisted outcome tracking of one manifest entry.
// Spec/Desired carry the declared target so a restart can re-derive or
// re-drive convergence even if the operation record is no longer around.
type BatchItem struct {
	Index  int             `json:"index"`
	Name   string          `json:"name"`
	Action ManifestAction  `json:"action"`
	EnvID  string          `json:"envId,omitempty"`
	Spec   *EnvSpec        `json:"spec,omitempty"`
	Desired Desired        `json:"desired,omitempty"`
	OpIDs  []string        `json:"opIds,omitempty"`
	Status BatchItemStatus `json:"status"`
	Reason string          `json:"reason,omitempty"`
	Note   string          `json:"note,omitempty"`
}

// Batch is the persisted record of one manifest application. It survives
// restarts so an interrupted apply keeps converging and stays explainable.
type Batch struct {
	ID        string       `json:"id"`
	TenantID  string       `json:"tenantId"`
	Actor     string       `json:"actor"`
	Items     []*BatchItem `json:"items"`
	Done      bool         `json:"done"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

// Terminal reports whether the item reached a final state.
func (bi *BatchItem) Terminal() bool {
	return bi.Status == BatchItemApplied || bi.Status == BatchItemFailed
}

// ChangeKind classifies what one manifest item would do to an environment.
type ChangeKind string

const (
	ChangeCreate ChangeKind = "Create"
	ChangeUpdate ChangeKind = "Update"
	ChangeDelete ChangeKind = "Delete"
	ChangeNoOp   ChangeKind = "NoOp"
)

// ItemPreview explains one manifest entry without touching any state.
type ItemPreview struct {
	Index         int         `json:"index"`
	Name          string      `json:"name"`
	Kind          ChangeKind  `json:"kind"`
	EnvID         string      `json:"envId,omitempty"`
	Note          string      `json:"note,omitempty"`
	Before        *EnvSpec    `json:"before,omitempty"`
	After         *EnvSpec    `json:"after,omitempty"`
	DesiredBefore Desired     `json:"desiredBefore,omitempty"`
	DesiredAfter  Desired     `json:"desiredAfter,omitempty"`
	Diff          []string    `json:"diff,omitempty"`
	Delta         Resources   `json:"delta"`
	ErrorKind     string      `json:"errorKind,omitempty"`
	Error         string      `json:"error,omitempty"`
}

// ManifestPreview is the read-only result of previewing a manifest: per-item
// classification with before/after diffs plus the net reservation change the
// whole batch would cause at tenant and cluster level.
type ManifestPreview struct {
	ManifestID      string        `json:"manifestId"`
	Items           []ItemPreview `json:"items"`
	TenantDelta     Resources     `json:"tenantDelta"`
	ClusterDelta    Resources     `json:"clusterDelta"`
	Admissible      bool          `json:"admissible"`
	AdmissionReason string        `json:"admissionReason,omitempty"`
}

// DiffSpecs renders the human-readable key-field differences between two
// specs (nil = absent). Desired transitions are appended by the caller.
func DiffSpecs(before, after *EnvSpec) []string {
	var out []string
	if before == nil && after != nil {
		out = append(out, "name: <absent> -> "+after.Name)
		out = append(out, "image: <absent> -> "+after.Image)
		out = append(out, "replicas: <absent> -> "+strconv.Itoa(after.Replicas))
		out = append(out, "ttlSeconds: <absent> -> "+strconv.Itoa(after.TTLSeconds))
		return out
	}
	if before != nil && after == nil {
		out = append(out, "name: "+before.Name+" -> <absent>")
		return out
	}
	if before == nil || after == nil {
		return out
	}
	if before.Image != after.Image {
		out = append(out, "image: "+before.Image+" -> "+after.Image)
	}
	if before.Replicas != after.Replicas {
		out = append(out, "replicas: "+strconv.Itoa(before.Replicas)+" -> "+strconv.Itoa(after.Replicas))
	}
	if before.Resources != after.Resources {
		out = append(out, "resources: "+before.Resources.String()+" -> "+after.Resources.String())
	}
	if before.TTLSeconds != after.TTLSeconds {
		out = append(out, "ttlSeconds: "+strconv.Itoa(before.TTLSeconds)+" -> "+strconv.Itoa(after.TTLSeconds))
	}
	return out
}
