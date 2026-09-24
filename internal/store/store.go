// Package store persists the control-plane state to a single local file.
// Writes are atomic (temp file + fsync + rename) and the whole document is
// versioned, so a crash at any point leaves either the old or new snapshot,
// never a torn document. No external services or credentials are involved.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// FileStore is a JSON-file backed, whole-snapshot persistent store.
type FileStore struct {
	path string
}

// NewFileStore creates a store rooted at path.
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Path returns the configured state file path.
func (s *FileStore) Path() string { return s.path }

// Load reads and decodes the snapshot. A missing file yields an initialized
// empty state, which makes first boot and a fresh directory indistinguishable.
func (s *FileStore) Load() (*domain.State, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return newState(), nil
		}
		return nil, fmt.Errorf("load state: %w", err)
	}
	if len(data) == 0 {
		return newState(), nil
	}
	st := newState()
	if err := json.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("decode state file %s: %w", s.path, err)
	}
	if st.Envs == nil {
		st.Envs = map[domain.EnvID]*domain.Environment{}
	}
	return st, nil
}

// Save atomically persists st: marshal -> temp file in the same directory ->
// fsync -> rename over the target -> fsync directory. A crash leaves either
// the previous complete file or the new complete file.
func (s *FileStore) Save(st *domain.State) error {
	if st == nil {
		return errors.New("cannot persist nil state")
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp state: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if we return early before rename.
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync temp state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("rename state into place: %w", err)
	}
	cleanup = false
	// fsync the directory so the rename itself is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func newState() *domain.State {
	return &domain.State{
		Envs:         map[domain.EnvID]*domain.Environment{},
		Dedup:        map[string]domain.DedupRecord{},
		TenantLimits: map[domain.TenantID]domain.Resources{},
	}
}
