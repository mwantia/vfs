package vfs

import (
	"context"
	"fmt"
	"strings"

	traversal "github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/errors"
	"github.com/mwantia/vfs/mount"
)

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (vfs *virtualFileSystemImpl) Mount(ctx context.Context, path string, steps ...mount.BuildStep) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}

	travel := traversal.WithAbsolute(ctx, path)
	// Handle root mount separately
	if strings.TrimSpace(path) == "/" {
		mounter, err := mount.BuildMounter(steps...)
		if err != nil {
			return fmt.Errorf("failed to build mounter for path '%s': %v", path, err)
		}

		root, err := mounter.Build(ctx, "/")
		if err != nil {
			return fmt.Errorf("failed to build mount for path '%s': %v", path, err)
		}

		return vfs.setRootMountPoint(root)
	}
	// All other mounts are traversed towards the correct mountpoint
	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootMount
	}

	return root.Mount(travel, steps...)
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (vfs *virtualFileSystemImpl) Unmount(ctx context.Context, path string, force bool) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}

	// Handle root mount separately
	path = strings.TrimSpace(path)
	if strings.TrimSpace(path) == "/" {
		//
		vfs.mu.Lock()
		defer vfs.mu.Unlock()
		// Fail if root is unmounted
		if vfs.root == nil {
			return fmt.Errorf("root is not mounted")
		}

		travel := traversal.WithAbsolute(ctx, path)
		if err := vfs.root.Shutdown(travel); err != nil {
			return err
		}

		vfs.root = nil
		return nil
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootUnmount
	}

	return root.Unmount(traversal.WithAbsolute(ctx, path), force)
}
