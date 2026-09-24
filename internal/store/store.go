// Package store persists the single control-plane State document to a local
// file with atomic write+fsync and read-after-write recovery semantics.
//
// Durability strategy: encode to a sibling temp file, fsync it, then rename
// over the target and fsync the directory. A crash at any point leaves
// either the previous complete file or the new complete file — never a torn
// document. No real database, cluster or credentials are involved.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
)

// Store is the local persistence boundary.
type Store struct {
	path string
	mu   sync.RWMutex
}

// New opens (or lazily creates) the file-backed store at path.
func New(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("store: empty path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	return &Store{path: path}, nil
}

// Load reads and decodes the persisted state; returns an empty state when
// the file does not yet exist.
func (s *Store) Load() (*model.State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return model.NewState(), nil
		}
		return nil, fmt.Errorf("store: read: %w", err)
	}
	if len(data) == 0 {
		return model.NewState(), nil
	}
	st := model.NewState()
	if err := json.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("store: decode: %w", err)
	}
	if st.Envs == nil {
		st.Envs = map[string]*model.Environment{}
	}
	if st.Quotas == nil {
		st.Quotas = map[string]*model.TenantQuota{}
	}
	return st, nil
}

// Save encodes and atomically replaces the state file.
func (s *Store) Save(st *model.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode: %w", err)
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("store: temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: write: %w", err)
	}
	if err := tmp.Sync(); err != nil { // flush file contents
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: fsync file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("store: close: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil { // atomic replace
		cleanup()
		return fmt.Errorf("store: rename: %w", err)
	}
	// Best-effort fsync of the directory so the rename entry is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Path returns the backing file path.
func (s *Store) Path() string { return s.path }
