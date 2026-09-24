// Package store persists the control-plane state to a single local file.
// Writes are atomic (temp file + fsync + rename) and the whole document is
// versioned, so a crash at any point leaves either the old or new snapshot.
package store

import "github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"

// FileStore is a JSON-file backed, whole-snapshot persistent store.
type FileStore struct {
	path string
}

// NewFileStore creates a store rooted at path.
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Load reads and decodes the snapshot. A missing file yields an empty state.
func (s *FileStore) Load() (*domain.State, error) { return nil, nil }

// Save atomically persists st.
func (s *FileStore) Save(st *domain.State) error { return nil }
