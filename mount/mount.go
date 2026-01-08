package mount

import (
	"context"
	"fmt"
	"strings"

	traversal "github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/errors"
	"github.com/mwantia/vfs/mount/builder"
	"github.com/mwantia/vfs/mount/extensions"
	"github.com/mwantia/vfs/mount/service"
)

// Health returns the basic and fastest result to check the lifecycle and availablility of this Service.
func (m *Mount) Health() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check object-storage (required) - must exist
	if m.ObjectStorage == nil {
		return false
	}
	// Check ObjectStorage health via Lifecycle
	if lifecycle := m.ObjectStorage.GetLifecycle(); lifecycle != nil {
		if ok, _ := lifecycle.Health(); !ok {
			return false
		}
	}

	// Check metadata (optional) - if exists, must be healthy
	if m.Metadata != nil {
		if lifecycle := m.Metadata.GetLifecycle(); lifecycle != nil {
			if ok, _ := lifecycle.Health(); !ok {
				return false
			}
		}
	}

	// Check extensions (optional) - if exist, must be healthy
	for _, extension := range m.Extensions {
		if extension != nil {
			if lifecycle := extension.GetLifecycle(); lifecycle != nil {
				if ok, _ := lifecycle.Health(); !ok {
					return false
				}
			}
		}
	}

	// Mount is healthy
	return true
}

// IsBusy checks if this mount or any of its services are currently busy.
// Returns true if it's NOT safe to unmount (i.e., services are in use).
func (m *Mount) IsBusy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if ObjectStorage is busy
	if m.ObjectStorage != nil {
		if lifecycle := m.ObjectStorage.GetLifecycle(); lifecycle != nil {
			if lifecycle.IsBusy() {
				return true
			}
		}
	}

	// Check if Metadata is busy
	if m.Metadata != nil {
		if lifecycle := m.Metadata.GetLifecycle(); lifecycle != nil {
			if lifecycle.IsBusy() {
				return true
			}
		}
	}

	// Check if any extension is busy
	for _, ext := range m.Extensions {
		if ext != nil {
			if lifecycle := ext.GetLifecycle(); lifecycle != nil {
				if lifecycle.IsBusy() {
					return true
				}
			}
		}
	}

	return false
}

// Shutdown unmounts all mounted filesystems and releases all resources.
// This should be called when shutting down the VFS to ensure proper cleanup.
// Mounts are unmounted in reverse order (deepest first) to avoid dependency issues.
func (m *Mount) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Collect all mount paths and sort them by depth (deepest first)
	var paths []string
	for path := range m.fstab {
		paths = append(paths, path)
	}
	// Sort by length (longer paths are deeper in the tree)
	for i := 0; i < len(paths); i++ {
		for j := i + 1; j < len(paths); j++ {
			if len(paths[j]) > len(paths[i]) {
				paths[i], paths[j] = paths[j], paths[i]
			}
		}
	}
	// Unmount all child filesystems
	var lastErr error
	for _, path := range paths {
		if mount, exists := m.fstab[path]; exists {
			if err := mount.Shutdown(ctx); err != nil {
				lastErr = err
				// Continue trying to unmount others even if one fails
			}
			delete(m.fstab, path)
		}
	}

	// Release all driver references (this will call CloseDriver when refcount reaches 0)
	// Services are wrappers around drivers, so we don't need to close them explicitly
	for _, uri := range m.uris {
		if err := service.ReleaseDriver(ctx, uri); err != nil && lastErr == nil {
			lastErr = err
		}
	}

	// Clear service references
	m.uris = nil
	m.ObjectStorage = nil
	m.Metadata = nil
	m.Extensions = nil

	return lastErr
}

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (m *Mount) Mount(ctx context.Context, path string, steps ...builder.MountStep) error {
	//
	if mount, mountpoint := m.resolveMountPoint(path); path != mountpoint {
		return mount.Mount(ctx, mountpoint)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()
	// Build the mount
	mounter, err := builder.BuildMounter(steps...)
	if err != nil {
		return fmt.Errorf("failed to build mounter: %v", err)
	}

	mount, err := BuildMount(ctx, path, mounter)
	if err != nil {
		return fmt.Errorf("failed to build mountpoint: %v", err)
	}

	// Add to fstab
	m.fstab[path] = mount
	// Persist via MountExtension if available
	if ext, ok := m.Extensions[service.ServiceExtensionMount]; ok {
		if mountExt, ok := ext.(extensions.MountExtension); ok {
			spec := mounter.ToMountSpec()
			if err := mountExt.SaveMount(ctx, path, spec); err != nil {
				// Rollback: remove from fstab
				delete(m.fstab, path)
				return fmt.Errorf("failed to persist mount: %v", err)
			}
		}
	}

	return nil
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (m *Mount) Unmount(ctx context.Context, path string, force bool) error {
	//
	if mount, mountpoint := m.resolveMountPoint(path); path != mountpoint {
		return mount.Unmount(ctx, mountpoint, force)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	mount, exists := m.fstab[path]
	if !exists {
		return errors.ErrPathNotMounted
	}

	// Check if mount is busy (unless forced)
	if !force && mount.IsBusy() {
		return errors.ErrMountBusy
	}
	// Check for child mounts in the mount's own fstab
	if !force && len(mount.fstab) > 0 {
		return errors.ErrMountBusy
	}

	if err := mount.Shutdown(ctx); err != nil {
		return err
	}
	// Delete persisted mount spec via MountExtension if available
	if ext, ok := m.Extensions[service.ServiceExtensionMount]; ok {
		if mount, ok := ext.(extensions.MountExtension); ok {
			// Best effort deletion - mount is already unmounted
			// so we don't want to fail the operation if persistence deletion fails
			if err := mount.DeleteMount(ctx, path); err != nil {
				return err
			}
			// Remove from fstab
			delete(m.fstab, path)
		}
	} else {
		// Remove from fstab
		delete(m.fstab, path)
	}

	return nil
}

func (m *Mount) Restore(ctx context.Context) error {
	errs := errors.Errors{}

	if ext, ok := m.Extensions[service.ServiceExtensionMount]; ok {
		if mount, ok := ext.(extensions.MountExtension); ok {
			// List all persisted mount specifications
			specs, err := mount.ListMounts(ctx)
			if err != nil {
				return fmt.Errorf("failed to list persisted mounts: %v", err)
			}
			// Restore each spec to mount
			for path, spec := range specs {
				// Skip any invalid specs
				if path == "" {
					continue
				}
				// Convert spec into mountsteps
				steps, err := spec.ToMountSteps()
				if err != nil {
					errs.Add(fmt.Errorf("failed to parse mount steps for '%s': %v", path, err))
					continue
				}
				// Mount steps using the persisted configuration and path
				if err := m.restoreMountPoint(ctx, path, steps...); err != nil {
					errs.Add(fmt.Errorf("failed to restore mountpoint for '%s': %v", path, err))
					continue
				}
			}
		}
	}

	return errs.Errors()
}

func (m *Mount) restoreMountPoint(ctx context.Context, path string, steps ...builder.MountStep) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	mounter, err := builder.BuildMounter(steps...)
	if err != nil {
		return fmt.Errorf("failed to build mounter for '%s': %v", path, err)
	}

	mount, err := BuildMount(ctx, path, mounter)
	if err != nil {
		return fmt.Errorf("failed to build mountpath for '%s': %v", path, err)
	}

	m.fstab[path] = mount
	return nil
}

// resolve
func (m *Mount) resolve(traversal traversal.TraversalContext) (bool, MountPoint, traversal.TraversalContext) {
	// Ignore empty or direct relative paths,
	// these are handled locally
	relative := traversal.RelativePath()
	if relative != "" && relative != "/" {
		m.mu.RLock()
		defer m.mu.RUnlock()

		var bestMatch MountPoint
		var bestPrefix string

		for path, mount := range m.fstab {
			// Normalize paths for comparison
			normalizedPath := strings.Trim(path, "/")
			normalizedRelative := strings.Trim(relative, "/")
			// Check if mountpath is a matching prefix
			if normalizedRelative == normalizedPath || strings.HasPrefix(normalizedRelative, normalizedPath+"/") {
				// Keep the longest match
				if len(normalizedPath) > len(bestPrefix) {
					bestMatch = mount
					bestPrefix = normalizedPath
				}
			}
		}
		// Found a matching submount to traverse to
		if bestPrefix != "" {
			return true, bestMatch, traversal.Traverse(bestPrefix)
		}
	}
	// No submount found, handle locally
	return false, m, traversal
}
