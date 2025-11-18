package mount

import (
	"sync"

	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/pkg/context"
	"github.com/mwantia/vfs/pkg/mount/backend"
)

type MountPoint struct {
	mu sync.RWMutex
}

// Health returns the basic and fastest result to check the lifecycle and availablility of this backend.
func (mp *MountPoint) Health() bool {
	return false
}

// Shutdown unmounts all mounted filesystems and releases all resources.
// This should be called when shutting down the VFS to ensure proper cleanup.
// Mounts are unmounted in reverse order (deepest first) to avoid dependency issues.
func (mp *MountPoint) Shutdown(ctx context.TraversalContext) error {
	return nil
}

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (mp *MountPoint) Mount(ctx context.TraversalContext, primary backend.ObjectStorageBackend, opts ...mount.MountOption) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	return nil
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (mp *MountPoint) Unmount(ctx context.TraversalContext, force bool) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	return nil
}
