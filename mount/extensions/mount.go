package extensions

import (
	"context"

	"github.com/mwantia/vfs/mount/builder"
	"github.com/mwantia/vfs/mount/service"
)

// MountExtension provides persistent mount management capabilities.
// Services implementing this interface can persist mount configurations
// to be restored on startup (e.g., SQLite-backed mount table).
//
// The extension is purely a storage layer - it saves and retrieves MountSpec data.
// The Mount struct itself handles rebuilding its fstab by calling ListMounts() and
// re-mounting each spec.
type MountExtension interface {
	// Service
	service.Service

	// SaveMount persists a mount specification at the given path.
	// The spec contains all configuration needed to rebuild the mount.
	SaveMount(ctx context.Context, path string, spec builder.MountSpecifications) error

	// LoadMount retrieves a persisted mount specification for the given path.
	// Returns error if no mount spec exists at the path.
	LoadMount(ctx context.Context, path string) (builder.MountSpecifications, error)

	// DeleteMount removes a persisted mount specification.
	// This is called when unmounting to clean up persistence.
	DeleteMount(ctx context.Context, path string) error

	// ListMounts returns all persisted mount specifications.
	// The Mount struct uses this to rebuild its fstab on initialization.
	ListMounts(ctx context.Context) (map[string]builder.MountSpecifications, error)
}
