package vfs

import (
	"context"
	"fmt"
	"strings"

	traversal "github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/errors"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/builder"
)

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (vfs *virtualFileSystemImpl) Mount(ctx context.Context, path string, steps ...builder.MountStep) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}
	// Handle root mount separately
	path = strings.TrimSpace(path)
	if path == "/" {
		mounter, err := builder.BuildMounter(steps...)
		if err != nil {
			return fmt.Errorf("failed to build mounter for path '%s': %v", path, err)
		}

		root, err := mount.BuildMount(ctx, "/", mounter)
		if err != nil {
			return fmt.Errorf("failed to build mount for path '%s': %v", path, err)
		}

		if err := vfs.setRootMountPoint(root); err != nil {
			return err
		}

		// Restore persisted mounts if MountExtension is available
		if err := root.Restore(ctx); err != nil {
			return fmt.Errorf("failed to restore persisted mounts: %v", err)
		}

		return nil
	}
	// All other mounts are traversed towards the correct mountpoint
	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootMount
	}

	return root.Mount(ctx, path, steps...)
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
	if path == "/" {
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

	return root.Unmount(ctx, path, force)
}

// ListMountSpecs returns all persisted mount specifications.
func (vfs *virtualFileSystemImpl) ListMountSpecs(ctx context.Context) (map[string]builder.MountSpecifications, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return nil, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return nil, errors.ErrRootNotMounted
	}

	return root.ListMountSpecs(ctx)
}

// GetMountSpec retrieves the mount specification for the given path.
func (vfs *virtualFileSystemImpl) GetMountSpec(ctx context.Context, path string) (builder.MountSpecifications, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return builder.MountSpecifications{}, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return builder.MountSpecifications{}, errors.ErrRootNotMounted
	}

	return root.GetMountSpec(ctx, path)
}

// UpdateMountSpec updates the mount specification for the given path.
func (vfs *virtualFileSystemImpl) UpdateMountSpec(ctx context.Context, path string, update builder.MountSpecUpdate) (builder.MountSpecifications, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return builder.MountSpecifications{}, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return builder.MountSpecifications{}, errors.ErrRootNotMounted
	}

	return root.UpdateMountSpec(ctx, path, update)
}
