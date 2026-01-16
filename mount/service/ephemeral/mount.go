package ephemeral

import (
	"context"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/builder"
	"github.com/mwantia/vfs/mount/service"
)

func (s *EphemeralMountExtensionService) GetLifecycle() service.Lifecycle {
	return s.driver
}

// PersistMountSpec persists a mount specification at the specified path.
// For ephemeral storage, this is in-memory only and will not survive restarts.
func (s *EphemeralMountExtensionService) PersistMountSpec(ctx context.Context, path string, spec builder.MountSpecifications) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	s.driver.mounts[path] = spec
	return nil
}

// RestoreMountSpec retrieves a persistent mount specification for the specified path.
// Returns error if no mount spec exists.
func (s *EphemeralMountExtensionService) RestoreMountSpec(ctx context.Context, path string) (builder.MountSpecifications, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	spec, ok := s.driver.mounts[path]
	if !ok {
		return builder.MountSpecifications{}, data.ErrNotExist
	}
	return spec, nil
}

// UpdateMountSpec updates an existing persistent mount specification.
// The update mask specifies which fields to modify.
func (s *EphemeralMountExtensionService) UpdateMountSpec(ctx context.Context, path string, update builder.MountSpecUpdate) (builder.MountSpecifications, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	existing, ok := s.driver.mounts[path]
	if !ok {
		return builder.MountSpecifications{}, data.ErrNotExist
	}

	// Apply the update
	update.Apply(&existing)
	s.driver.mounts[path] = existing

	return existing, nil
}

// DeleteMountSpec removes a persisted mount specification.
// This is called when unmounting to clean up persistence.
func (s *EphemeralMountExtensionService) DeleteMountSpec(ctx context.Context, path string) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	delete(s.driver.mounts, path)
	return nil
}

// ListAllMountSpecs returns all persisted mount specifications.
// The Mount struct uses this to rebuild its fstab on initialization.
func (s *EphemeralMountExtensionService) ListAllMountSpecs(ctx context.Context) (map[string]builder.MountSpecifications, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Return a copy to avoid external modification
	result := make(map[string]builder.MountSpecifications, len(s.driver.mounts))
	for k, v := range s.driver.mounts {
		result[k] = v
	}
	return result, nil
}
