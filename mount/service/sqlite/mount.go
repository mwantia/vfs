package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/builder"
	"github.com/mwantia/vfs/mount/service"
)

func (s *SqliteMountExtensionService) GetLifecycle() service.Lifecycle {
	return s.driver
}

// SaveMount persists a mount specification at the given path.
// The spec contains all configuration needed to rebuild the mount.
func (s *SqliteMountExtensionService) SaveMount(ctx context.Context, path string, spec builder.MountSpecifications) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Serialize MountSpec to JSON
	buf, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	// Insert or replace mount spec in database
	_, err = s.driver.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO vfs_mounts (path, object_storage, metadata, spec)
		VALUES (?, ?, ?, ?)
	`, path, spec.ObjectStorage, spec.Metadata, string(buf))

	return err
}

// LoadMount retrieves a persisted mount specification for the given path.
// Returns error if no mount spec exists at the path.
func (s *SqliteMountExtensionService) LoadMount(ctx context.Context, path string) (builder.MountSpecifications, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Query mount spec from database
	var scanSpec string
	err := s.driver.db.QueryRowContext(ctx,
		"SELECT spec FROM vfs_mounts WHERE path = ?", path).Scan(&scanSpec)

	if err == sql.ErrNoRows {
		return builder.MountSpecifications{}, data.ErrNotExist
	}
	if err != nil {
		return builder.MountSpecifications{}, err
	}

	// Deserialize JSON to MountSpec
	var spec builder.MountSpecifications
	if err := json.Unmarshal([]byte(scanSpec), &spec); err != nil {
		return builder.MountSpecifications{}, err
	}

	return spec, nil
}

// DeleteMount removes a persisted mount specification.
// This is called when unmounting to clean up persistence.
func (s *SqliteMountExtensionService) DeleteMount(ctx context.Context, path string) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	// Delete mount spec from database
	_, err := s.driver.db.ExecContext(ctx,
		"DELETE FROM vfs_mounts WHERE path = ?", path)

	return err
}

// ListMounts returns all persisted mount specifications.
// The Mount struct uses this to rebuild its fstab on initialization.
func (s *SqliteMountExtensionService) ListMounts(ctx context.Context) (map[string]builder.MountSpecifications, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Query all mount specs from database
	rows, err := s.driver.db.QueryContext(ctx,
		"SELECT path, spec FROM vfs_mounts ORDER BY path")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Deserialize each spec
	specs := make(map[string]builder.MountSpecifications)
	for rows.Next() {
		var path, specJSON string
		if err := rows.Scan(&path, &specJSON); err != nil {
			return nil, err
		}

		var spec builder.MountSpecifications
		if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
			// Skip invalid specs
			continue
		}

		specs[path] = spec
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return specs, nil
}
