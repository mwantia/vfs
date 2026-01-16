package mount

import (
	"context"
	"fmt"

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

	mnt, err := BuildMount(ctx, path, mounter)
	if err != nil {
		return fmt.Errorf("failed to build mountpoint: %v", err)
	}

	// Add to fstab
	m.fstab[path] = mnt
	// Persist via MountExtension if available
	if ext, ok := m.Extensions[service.ServiceExtensionMount]; ok {
		if mount, ok := ext.(extensions.MountExtension); ok {
			spec := mounter.ToMountSpec()
			if err := mount.PersistMountSpec(ctx, path, spec); err != nil {
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

	mnt, exists := m.fstab[path]
	if !exists {
		return errors.ErrPathNotMounted
	}

	// Check if mount is busy (unless forced)
	if !force && mnt.IsBusy() {
		return errors.ErrMountBusy
	}
	// Check for child mounts in the mount's own fstab
	if !force && len(mnt.fstab) > 0 {
		return errors.ErrMountBusy
	}

	if err := mnt.Shutdown(ctx); err != nil {
		return err
	}
	// Delete persisted mount spec via MountExtension if available
	if ext, ok := m.Extensions[service.ServiceExtensionMount]; ok {
		if mount, ok := ext.(extensions.MountExtension); ok {
			// Best effort deletion - mount is already unmounted
			// so we don't want to fail the operation if persistence deletion fails
			if err := mount.DeleteMountSpec(ctx, path); err != nil {
				return fmt.Errorf("failed to delete persistent mount spec: %v", err)
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
			specs, err := mount.ListAllMountSpecs(ctx)
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

	mnt, err := BuildMount(ctx, path, mounter)
	if err != nil {
		return fmt.Errorf("failed to build mountpath for '%s': %v", path, err)
	}

	m.fstab[path] = mnt
	return nil
}

// ListMountSpecs returns all persisted mount specifications.
// Returns an error if no MountExtension is configured.
func (m *Mount) ListMountSpecs(ctx context.Context) (map[string]builder.MountSpecifications, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ext, ok := m.Extensions[service.ServiceExtensionMount]
	if !ok {
		return nil, fmt.Errorf("no mount extension configured")
	}

	mountExt, ok := ext.(extensions.MountExtension)
	if !ok {
		return nil, fmt.Errorf("invalid mount extension type")
	}

	return mountExt.ListAllMountSpecs(ctx)
}

// GetMountSpec retrieves the mount specification for the given path.
// Returns an error if the path is not mounted or no MountExtension is configured.
func (m *Mount) GetMountSpec(ctx context.Context, path string) (builder.MountSpecifications, error) {
	// Check if path resolves to a child mount
	if mount, mountpoint := m.resolveMountPoint(path); path != mountpoint {
		return mount.GetMountSpec(ctx, mountpoint)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	ext, ok := m.Extensions[service.ServiceExtensionMount]
	if !ok {
		return builder.MountSpecifications{}, fmt.Errorf("no mount extension configured")
	}

	mountExt, ok := ext.(extensions.MountExtension)
	if !ok {
		return builder.MountSpecifications{}, fmt.Errorf("invalid mount extension type")
	}

	return mountExt.RestoreMountSpec(ctx, path)
}

// UpdateMountSpec updates the mount specification for the given path.
// Returns the updated specification or an error if the update fails.
func (m *Mount) UpdateMountSpec(ctx context.Context, path string, update builder.MountSpecUpdate) (builder.MountSpecifications, error) {
	// Check if path resolves to a child mount
	if mount, mountpoint := m.resolveMountPoint(path); path != mountpoint {
		return mount.UpdateMountSpec(ctx, mountpoint, update)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ext, ok := m.Extensions[service.ServiceExtensionMount]
	if !ok {
		return builder.MountSpecifications{}, fmt.Errorf("no mount extension configured")
	}

	mountExt, ok := ext.(extensions.MountExtension)
	if !ok {
		return builder.MountSpecifications{}, fmt.Errorf("invalid mount extension type")
	}

	return mountExt.UpdateMountSpec(ctx, path, update)
}
