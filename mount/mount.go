package mount

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/errors"
	"github.com/mwantia/vfs/mount/extensions"
	"github.com/mwantia/vfs/mount/service"
)

type Mount struct {
	mu         sync.RWMutex
	fstab      map[string]*Mount
	uris       []string
	createTime time.Time

	MountPoint string
	Options    *MountOptions

	ObjectStorage service.ObjectStorageService
	Metadata      service.MetadataService
	Extensions    map[service.ServiceExtension]service.Service
}

type MountOptions struct {
	Namespace  string
	PathPrefix string
	IsReadOnly bool
}

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
func (m *Mount) Shutdown(traversal context.TraversalContext) error {
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
			if err := mount.Shutdown(traversal); err != nil {
				lastErr = err
				// Continue trying to unmount others even if one fails
			}
			delete(m.fstab, path)
		}
	}

	// Release all driver references (this will call CloseDriver when refcount reaches 0)
	// Services are wrappers around drivers, so we don't need to close them explicitly
	for _, uri := range m.uris {
		if err := service.ReleaseDriver(traversal, uri); err != nil && lastErr == nil {
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
func (m *Mount) Mount(traversal context.TraversalContext, steps ...BuildStep) error {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.Mount(mountTraversal, steps...)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	relative := traversal.RelativePath()
	// Build the mount
	mounter, err := BuildMounter(steps...)
	if err != nil {
		return fmt.Errorf("failed to build mounter: %v", err)
	}

	mount, err := mounter.Build(traversal, relative)
	if err != nil {
		return fmt.Errorf("failed to build mountpoint: %v", err)
	}

	// Add to fstab
	m.fstab[relative] = mount
	// Persist via MountExtension if available
	if ext, ok := m.Extensions[service.ServiceExtensionMount]; ok {
		if mountExt, ok := ext.(extensions.MountExtension); ok {
			spec := mounter.ToSpec()
			if err := mountExt.SaveMount(traversal, spec); err != nil {
				// Rollback: remove from fstab
				delete(m.fstab, relative)
				return fmt.Errorf("failed to persist mount: %v", err)
			}
		}
	}

	return nil
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (m *Mount) Unmount(traversal context.TraversalContext, force bool) error {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.Unmount(mountTraversal, force)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	relative := traversal.RelativePath()
	mount, exists := m.fstab[relative]
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

	if err := mount.Shutdown(traversal); err != nil {
		return err
	}
	// Delete persisted mount spec via MountExtension if available
	if ext, ok := m.Extensions[service.ServiceExtensionMount]; ok {
		if mountExt, ok := ext.(extensions.MountExtension); ok {
			// Best effort deletion - mount is already unmounted
			// so we don't want to fail the operation if persistence deletion fails
			if err := mountExt.DeleteMount(traversal); err != nil {
				return err
			}
			// Remove from fstab
			delete(m.fstab, relative)
		}
	} else {
		// Remove from fstab
		delete(m.fstab, relative)
	}

	return nil
}

// RestoreMounts rebuilds the fstab from persisted mount specifications.
// This is called during mount initialization (e.g., after VFS restart).
// It uses the MountExtension (like /etc/fstab) to retrieve all saved mounts
// and re-mounts them.
func (m *Mount) RestoreMounts(traversal context.TraversalContext) error {
	// Check if we have mount extension
	if ext, ok := m.Extensions[service.ServiceExtensionMount]; ok {
		if mountExt, ok := ext.(extensions.MountExtension); ok {
			// List all persisted mount specs
			specs, err := mountExt.ListMounts(traversal)
			if err != nil {
				return fmt.Errorf("failed to list persisted mounts: %v", err)
			}
			// Restore each mount
			for _, specInterface := range specs {
				// Cast back to *MountSpec
				spec, ok := specInterface.(*MountSpec)
				if !ok {
					continue // Skip invalid specs
				}
				// Convert spec to BuildSteps
				steps, err := spec.ToSteps()
				if err != nil {
					// Log but continue with other mounts
					continue
				}
				// Mount using the persisted configuration
				// Note: This will NOT call SaveMount again since the spec is already persisted
				// We need to prevent double-saving
				if err := m.restoreMount(traversal, steps); err != nil {
					// Log but continue with other mounts
					continue
				}
			}
		}
	}

	return nil
}

// restoreMount is like Mount() but skips persistence (used during restoration)
func (m *Mount) restoreMount(traversal context.TraversalContext, steps []BuildStep) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	relative := traversal.RelativePath()
	// Build the mount
	mounter, err := BuildMounter(steps...)
	if err != nil {
		return fmt.Errorf("failed to build mounter: %v", err)
	}

	mount, err := mounter.Build(traversal, relative)
	if err != nil {
		return fmt.Errorf("failed to build mountpoint: %v", err)
	}
	// Add to fstab (don't persist - it's already persisted)
	m.fstab[relative] = mount

	return nil
}

// isReadonly
func (m *Mount) isReadonly() bool {
	return m.Options.IsReadOnly
}

// resolve
func (m *Mount) resolve(traversal context.TraversalContext) (bool, MountPoint, context.TraversalContext) {
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
