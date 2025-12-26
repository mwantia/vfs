package sqlite

import (
	"database/sql"
	"encoding/json"

	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/extensions"
	"github.com/mwantia/vfs/mount/service"
)

func (s *SqliteMountExtensionService) GetLifecycle() service.Lifecycle {
	return s.driver
}

// SaveMount persists a mount specification at the given path.
// The spec contains all configuration needed to rebuild the mount.
func (s *SqliteMountExtensionService) SaveMount(traversal context.TraversalContext, spec extensions.MountSpec) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Get the mount path from traversal context
	path := traversal.AbsolutePath()

	// Serialize MountSpec to JSON
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	// Insert or replace mount spec in database
	_, err = s.driver.db.ExecContext(traversal, `
		INSERT OR REPLACE INTO vfs_mounts (path, spec)
		VALUES (?, ?)
	`, path, string(specJSON))

	return err
}

// LoadMount retrieves a persisted mount specification for the given path.
// Returns error if no mount spec exists at the path.
func (s *SqliteMountExtensionService) LoadMount(traversal context.TraversalContext) (extensions.MountSpec, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Get the mount path from traversal context
	path := traversal.AbsolutePath()

	// Query mount spec from database
	var specJSON string
	err := s.driver.db.QueryRowContext(traversal,
		"SELECT spec FROM vfs_mounts WHERE path = ?", path).Scan(&specJSON)

	if err == sql.ErrNoRows {
		return nil, data.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	// Deserialize JSON to MountSpec
	var spec extensions.MountSpec
	if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
		return nil, err
	}

	return spec, nil
}

// DeleteMount removes a persisted mount specification.
// This is called when unmounting to clean up persistence.
func (s *SqliteMountExtensionService) DeleteMount(traversal context.TraversalContext) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Get the mount path from traversal context
	path := traversal.AbsolutePath()

	// Delete mount spec from database
	_, err := s.driver.db.ExecContext(traversal,
		"DELETE FROM vfs_mounts WHERE path = ?", path)

	return err
}

// ListMounts returns all persisted mount specifications.
// The Mount struct uses this to rebuild its fstab on initialization.
func (s *SqliteMountExtensionService) ListMounts(traversal context.TraversalContext) ([]extensions.MountSpec, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Query all mount specs from database
	rows, err := s.driver.db.QueryContext(traversal,
		"SELECT spec FROM vfs_mounts ORDER BY path")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Deserialize each spec
	specs := make([]extensions.MountSpec, 0)
	for rows.Next() {
		var specJSON string
		if err := rows.Scan(&specJSON); err != nil {
			return nil, err
		}

		var spec extensions.MountSpec
		if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
			// Skip invalid specs
			continue
		}

		specs = append(specs, spec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return specs, nil
}
