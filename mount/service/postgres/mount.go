package postgres

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/builder"
	"github.com/mwantia/vfs/mount/service"
)

func (s *PostgresMountExtensionService) GetLifecycle() service.Lifecycle {
	return s.driver
}

// PersistMountSpec persists a mount specification at the specified path.
// The spec contains all required configuration needed to restore the mount later.
func (s *PostgresMountExtensionService) PersistMountSpec(ctx context.Context, path string, spec builder.MountSpecifications) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Serialize MountSpec to JSON
	buf, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	// Insert or update mount spec in database (upsert)
	_, err = s.driver.pool.Exec(ctx, `
		INSERT INTO vfs_mounts (path, spec)
		VALUES ($1, $2)
		ON CONFLICT (path) DO UPDATE SET spec = $2
	`, path, buf)

	return err
}

// RestoreMountSpec retrieves a persistent mount specification for the specified path.
// Returns error if no mount spec exists, or the stored configuration is corrupt.
func (s *PostgresMountExtensionService) RestoreMountSpec(ctx context.Context, path string) (builder.MountSpecifications, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Query mount spec from database
	var specJSON []byte
	err := s.driver.pool.QueryRow(ctx,
		"SELECT spec FROM vfs_mounts WHERE path = $1", path).Scan(&specJSON)

	if err == pgx.ErrNoRows {
		return builder.MountSpecifications{}, data.ErrNotExist
	}
	if err != nil {
		return builder.MountSpecifications{}, err
	}

	// Deserialize JSON to MountSpec
	var spec builder.MountSpecifications
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return builder.MountSpecifications{}, err
	}

	return spec, nil
}

// UpdateMountSpec updates an existing persistent mount specification.
// The update mask specifies which fields to modify.
func (s *PostgresMountExtensionService) UpdateMountSpec(ctx context.Context, path string, update builder.MountSpecUpdate) (builder.MountSpecifications, error) {
	// First, restore the existing spec
	existing, err := s.RestoreMountSpec(ctx, path)
	if err != nil {
		return builder.MountSpecifications{}, err
	}

	// Apply the update
	if !update.Apply(&existing) {
		// No changes made, return existing spec
		return existing, nil
	}

	// Persist the updated spec
	if err := s.PersistMountSpec(ctx, path, existing); err != nil {
		return builder.MountSpecifications{}, err
	}

	return existing, nil
}

// DeleteMountSpec removes a persisted mount specification.
// This is called when unmounting to clean up persistence.
func (s *PostgresMountExtensionService) DeleteMountSpec(ctx context.Context, path string) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Delete mount spec from database
	_, err := s.driver.pool.Exec(ctx,
		"DELETE FROM vfs_mounts WHERE path = $1", path)

	return err
}

// ListAllMountSpecs returns all persisted mount specifications.
// The Mount struct uses this to rebuild its fstab on initialization.
func (s *PostgresMountExtensionService) ListAllMountSpecs(ctx context.Context) (map[string]builder.MountSpecifications, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Query all mount specs from database
	rows, err := s.driver.pool.Query(ctx,
		"SELECT path, spec FROM vfs_mounts ORDER BY path")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Deserialize each spec
	specs := make(map[string]builder.MountSpecifications)
	for rows.Next() {
		var path string
		var specJSON []byte
		if err := rows.Scan(&path, &specJSON); err != nil {
			return nil, err
		}

		var spec builder.MountSpecifications
		if err := json.Unmarshal(specJSON, &spec); err != nil {
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
