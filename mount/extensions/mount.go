package extensions

import (
	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/mount/service"
)

// MountSpec is forward-declared to avoid import cycle
type MountSpec any

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
	SaveMount(traversal context.TraversalContext, spec MountSpec) error

	// LoadMount retrieves a persisted mount specification for the given path.
	// Returns error if no mount spec exists at the path.
	LoadMount(traversal context.TraversalContext) (MountSpec, error)

	// DeleteMount removes a persisted mount specification.
	// This is called when unmounting to clean up persistence.
	DeleteMount(traversal context.TraversalContext) error

	// ListMounts returns all persisted mount specifications.
	// The Mount struct uses this to rebuild its fstab on initialization.
	ListMounts(traversal context.TraversalContext) ([]MountSpec, error)
}
