package pkg

import (
	"context"
	"fmt"
	"strings"

	"github.com/mwantia/vfs/mount"
	tctx "github.com/mwantia/vfs/pkg/context"
	"github.com/mwantia/vfs/pkg/mount/backend"
)

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (vfs *virtualFileSystemImpl) Mount(ctx context.Context, path string, primary backend.ObjectStorageBackend, opts ...mount.MountOption) error {
	root, err := vfs.checkRootMount()
	if err != nil {
		return fmt.Errorf("failed to mount backend to filesystem: %w", err)
	}
	// Handle root mount separately
	if strings.TrimSpace(path) == "/" {
		// TODO :: Initialize and mount root
		return nil
	}
	// All other mounts are traversed towards the correct mountpoint
	return root.Mount(tctx.WithAbsolute(ctx, path), primary, opts...)
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (vfs *virtualFileSystemImpl) Unmount(ctx context.Context, path string, force bool) error {
	root, err := vfs.checkRootMount()
	if err != nil {
		return fmt.Errorf("failed to unmount backend to filesystem: %w", err)
	}

	return root.Unmount(tctx.WithAbsolute(ctx, path), force)
}
