package ephemeral

import (
	"context"

	"github.com/mwantia/vfs/mount/builder"
	"github.com/mwantia/vfs/mount/service"
)

func (s *EphemeralMountExtensionService) GetLifecycle() service.Lifecycle {
	return s.driver
}

// SaveMount persists a mount specification at the given path.
// The spec contains all configuration needed to rebuild the mount.
func (s *EphemeralMountExtensionService) SaveMount(ctx context.Context, spec builder.MountSpecifications) error {
	return nil
}

// LoadMount retrieves a persisted mount specification for the given path.
// Returns error if no mount spec exists at the path.
func (s *EphemeralMountExtensionService) LoadMount(ctx context.Context) (builder.MountSpecifications, error) {
	return builder.MountSpecifications{}, nil
}

// DeleteMount removes a persisted mount specification.
// This is called when unmounting to clean up persistence.
func (s *EphemeralMountExtensionService) DeleteMount(ctx context.Context) error {
	return nil
}

// ListMounts returns all persisted mount specifications.
// The Mount struct uses this to rebuild its fstab on initialization.
func (s *EphemeralMountExtensionService) ListMounts(ctx context.Context) ([]builder.MountSpecifications, error) {
	return nil, nil
}
