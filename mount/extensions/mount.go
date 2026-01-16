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

	// PersistMountSpec persists a mount specification at the specified path.
	// The spec contains all required configuration needed to restore the mount later.
	PersistMountSpec(ctx context.Context, path string, spec builder.MountSpecifications) error

	// RestoreMountSpec retrieves a persistent mount specification for the specificed path.
	// Returns error if no mount spec exists, or the stored configuration is corrupt.
	RestoreMountSpec(ctx context.Context, path string) (builder.MountSpecifications, error)

	// UpdateMountSpec updates an existing persistent mount specification.
	// The update mask specifies which fields to modify.
	// Returns the full updated spec after applying changes.
	UpdateMountSpec(ctx context.Context, path string, update builder.MountSpecUpdate) (builder.MountSpecifications, error)

	// DeleteMountSpec removes a persisted mount specification.
	// This is called when unmounting to clean up persistence.
	DeleteMountSpec(ctx context.Context, path string) error

	// ListAllMountSpecs returns all persisted mount specifications.
	// The Mount struct uses this to rebuild its fstab on initialization.
	ListAllMountSpecs(ctx context.Context) (map[string]builder.MountSpecifications, error)
}
