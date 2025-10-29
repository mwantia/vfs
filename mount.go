package vfs

import (
	"context"
	"fmt"
	"strings"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/backend"
)

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (vfs *virtualFileSystemImpl) Mount(ctx context.Context, path string, primary backend.VirtualObjectStorageBackend, opts ...mount.MountOption) error {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("Mount: failed to convert path to absolute: %s - %v", path, err)
		return err
	}

	vfs.log.Debug("Mount: mounting %s backend at %s", primary.Name(), absolute)

	if len(absolute) == 0 {
		vfs.log.Error("Mount: invalid empty path")
		return errors.InvalidPath(nil, absolute)
	}
	// Check if parent mount denies nesting BEFORE acquiring write lock
	if parent, err := vfs.getMountFromPath(absolute); err == nil {
		if !parent.Options.Nesting {
			vfs.log.Error("Mount: parent mount at %s denies nesting", parent.Path)
			return errors.PathMountNestingDenied(nil, parent.Path)
		}
		vfs.log.Debug("Mount: parent mount at %s allows nesting", parent.Path)
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	name := fmt.Sprintf("mount/%s", primary.Name())
	log := vfs.log.Named(name)

	mnt, err := mount.NewMountInfo(path, log, primary, opts...)
	if err != nil {
		vfs.log.Error("Mount: failed to create mount info for %s - %v", absolute, err)
		return err
	}

	if _, exists := vfs.mnts[absolute]; exists {
		vfs.log.Error("Mount: path %s is already mounted", absolute)
		return errors.PathAlreadyMounted(nil, absolute)
	}

	vfs.log.Debug("Mount: initializing mount at %s (readonly=%v dual=%v)", absolute, mnt.Options.ReadOnly, mnt.IsDualMount)
	if err := mnt.Mount(ctx); err != nil {
		vfs.log.Error("Mount: failed to mount %s backend at %s - %v", primary.Name(), absolute, err)
		return data.ErrMountFailed
	}

	vfs.mnts[absolute] = mnt
	vfs.log.Info("Mount: successfully mounted %s at %s", primary.Name(), absolute)
	return nil
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (vfs *virtualFileSystemImpl) Unmount(ctx context.Context, path string, force bool) error {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("Unmount: failed to convert path to absolute: %s - %v", path, err)
		return err
	}

	vfs.log.Debug("Unmount: unmounting %s (force=%v)", absolute, force)

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	mnt, exists := vfs.mnts[absolute]
	if !exists {
		vfs.log.Error("Unmount: path %s is not mounted", absolute)
		return errors.PathNotMounted(nil, absolute)
	}

	if vfs.hasChildMounts(absolute) {
		vfs.log.Error("Unmount: path %s has child mounts, cannot unmount", absolute)
		return errors.PathMountBusy(nil, absolute)
	}

	vfs.log.Debug("Unmount: closing mount at %s", absolute)
	if err := mnt.Unmount(ctx, force); err != nil {
		vfs.log.Error("Unmount: failed to unmount %s - %v", absolute, err)
		return data.ErrUnmountFailed
	}

	delete(vfs.mnts, absolute)
	vfs.log.Info("Unmount: successfully unmounted %s", absolute)
	return nil
}

func (vfs *virtualFileSystemImpl) getMountFromPath(path string) (*mount.Mount, error) {
	// Skip, if no entries have been mounted yet
	if len(vfs.mnts) == 0 {
		return nil, errors.PathNotMounted(nil, path)
	}

	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	var best *mount.Mount
	for point, mnt := range vfs.mnts {
		if data.HasPrefix(path, point) {
			// For root mount ("/"), it matches everything
			// For other mounts, ensure exact match or path continues with /
			if point == "/" || len(path) == len(point) || (len(path) > len(point) && path[len(point)] == '/') {
				if best == nil || len(point) > len(best.Path) {
					best = mnt
				}
			}
		}
	}

	if best == nil {
		return nil, errors.PathNotMounted(nil, path)
	}

	return best, nil
}

func (vfs *virtualFileSystemImpl) hasChildMounts(parent string) bool {
	for mount := range vfs.mnts {
		if mount != parent && data.HasPrefix(mount, parent) {
			return true
		}
	}

	return false
}

// getChildMounts returns all direct child mount points under the given path.
// For example, if path is "/" and mounts exist at "/data" and "/data/cache",
// only "/data" is returned (not "/data/cache" as it's a grandchild).
func (vfs *virtualFileSystemImpl) getChildMounts(path string) []string {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	children := make([]string, 0)

	// Normalize path - ensure it ends with / for prefix matching
	normalizedPath := path
	if path != "/" && !data.HasPrefix(normalizedPath, "/") {
		normalizedPath = "/" + normalizedPath
	}
	if path != "/" {
		normalizedPath = normalizedPath + "/"
	}

	for mountPath := range vfs.mnts {
		// Skip the path itself
		if mountPath == path {
			continue
		}

		// Skip if not a child of this path
		if !data.HasPrefix(mountPath, normalizedPath) {
			continue
		}

		// Get relative path after the parent
		relative := mountPath
		if path == "/" {
			relative = mountPath[1:] // Remove leading /
		} else {
			relative = mountPath[len(normalizedPath):]
		}

		// Only include direct children (no / in the relative path)
		if relative != "" && !strings.Contains(relative, "/") {
			children = append(children, mountPath)
		}
	}

	return children
}
