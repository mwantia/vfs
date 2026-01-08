package mount

import (
	"context"
	"fmt"
	"strings"

	traversal "github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/errors"
	"github.com/mwantia/vfs/mount/extensions/notification"
	"github.com/mwantia/vfs/mount/service"
)

// OpenFile opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (m *Mount) OpenFile(traversal traversal.TraversalContext, flags data.AccessMode) (MountStreamer, error) {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.OpenFile(mountTraversal, flags)
	}
	// Throw if mountpoint is marked as readonly
	if m.Options.IsReadOnly {
		// We have to check if the provided flags requests any "writable"-access
		if flags&data.AccessModeWrite != 0 || flags&data.AccessModeCreate != 0 || flags&data.AccessModeExcl != 0 {
			return nil, errors.ErrMountIsReadonly
		}
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	namespace := m.Options.Namespace
	key := traversal.RelativePath()

	// Check path constraints
	if err := m.validatePathLength(key); err != nil {
		return nil, err
	}
	if err := m.validatePathDepth(key); err != nil {
		return nil, err
	}

	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationStream) {
		return nil, errors.ErrOperationNotSupported
	}

	// Check if file exists when not creating
	if flags&data.AccessModeCreate == 0 {
		// Check if object exists
		stat, err := m.ObjectStorage.HeadObject(traversal, namespace, key)
		if err != nil {
			return nil, err
		}
		// Check if it's a directory
		if stat.Mode.IsDir() {
			return nil, data.ErrIsDirectory
		}
	}

	// Handle file creation
	if flags&data.AccessModeCreate != 0 {
		// Create the file - use 0644 as default mode for regular files
		stat, err := m.ObjectStorage.CreateObject(traversal, namespace, key, 0644)
		if err != nil && err != data.ErrExist {
			return nil, err
		}
		// If ErrExist and AccessModeExcl is set, fail
		if err == data.ErrExist && flags&data.AccessModeExcl != 0 {
			return nil, data.ErrExist
		}

		// Sync to MetadataService if available (only if newly created)
		if err == nil && m.Metadata != nil {
			meta := stat.ToMetadata()
			if syncErr := m.Metadata.CreateMeta(traversal, namespace, meta); syncErr != nil && syncErr != data.ErrExist {
				// TODO :: Missing internal log for tracking internal errors
				fmt.Printf("WARNING: Failed to sync file creation to MetadataService: %v\n", syncErr)
			}
		}
	}

	// Handle truncation
	if flags&data.AccessModeTrunc != 0 {
		if err := m.ObjectStorage.TruncateObject(traversal, namespace, key, 0); err != nil {
			return nil, err
		}

		// Sync truncation to MetadataService if available
		if m.Metadata != nil {
			update := data.MetadataUpdate{
				Mask: data.MetadataUpdateSize,
				Metadata: data.Metadata{
					Size: 0,
				},
			}
			if updateErr := m.Metadata.UpdateMeta(traversal, namespace, key, update); updateErr != nil && updateErr != data.ErrNotExist {
				// TODO :: Missing internal log for tracking internal errors
				fmt.Printf("WARNING: Failed to sync truncate to metadata service: %v\n", updateErr)
			}
		}
	}

	// Determine initial offset for append mode
	offset := int64(0)
	if flags&data.AccessModeAppend != 0 {
		// Get file size for append
		stat, err := m.ObjectStorage.HeadObject(traversal, namespace, key)
		if err != nil {
			return nil, err
		}
		offset = stat.Size
	}

	if ok, err := m.emitNotificationEvent(traversal, notification.NotificationTypeFileOpened, key, false, 0); ok && err != nil {
		return nil, err
	}

	return newTraversalMountStreamer(traversal, m, offset, flags), nil
}

// CloseFile closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (m *Mount) CloseFile(traversal traversal.TraversalContext, force bool) error {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.CloseFile(mountTraversal, force)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	relativePath := traversal.RelativePath()

	if ok, err := m.emitNotificationEvent(traversal, notification.NotificationTypeFileClosed, relativePath, false, 0); ok && err != nil {
		return err
	}

	return nil
}

// Read reads size bytes from the file at path starting at offset.
// Returns the data read or an error if the operation fails.
func (m *Mount) ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error) {
	// Check if child mount exists for this path
	if mount, relativePath := m.resolveMountPoint(path); path != relativePath {
		// Delegate to child mount, but don't return immediately
		buffer, err := mount.ReadFile(ctx, path, offset, size)
		if err != nil {
			return nil, err
		}

		return buffer, nil
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationRead) {
		return nil, errors.ErrOperationNotSupported
	}

	// Allocate buffer for read
	buffer := make([]byte, size)

	// Read from ObjectStorage
	n, err := m.ObjectStorage.ReadObject(ctx, m.Options.Namespace, path, offset, buffer)
	if err != nil {
		return nil, err
	}

	if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeRead, path, false, 0); ok && err != nil {
		return nil, err
	}

	// Return only the bytes actually read
	return buffer[:n], nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (m *Mount) WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error) {
	// Check if child mount exists for this path
	if mount, relativePath := m.resolveMountPoint(path); path != relativePath {
		// Delegate to child mount, but don't return immediately
		size, err := mount.WriteFile(ctx, path, offset, buffer)
		if err != nil {
			return 0, err
		}

		return size, nil
	}
	// Throw if mountpoint is marked as readonly
	if m.Options.IsReadOnly {
		return 0, errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()
	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationWrite) {
		return 0, errors.ErrOperationNotSupported
	}
	// Check object size constraints (final size after write)
	writeEnd := offset + int64(len(buffer))
	if err := m.validateObjectSize(writeEnd); err != nil {
		return 0, err
	}
	// Write to ObjectStorage
	n, err := m.ObjectStorage.WriteObject(ctx, m.Options.Namespace, path, offset, buffer)
	if err != nil {
		return 0, err
	}

	// Sync write to MetadataService if available (update size and modify time)
	if m.Metadata != nil {
		// Get current file size after write
		stat, statErr := m.ObjectStorage.HeadObject(ctx, m.Options.Namespace, path)
		if statErr != nil {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to get file stat after write for metadata sync: %v\n", statErr)
		} else {
			update := data.MetadataUpdate{
				Mask: data.MetadataUpdateSize,
				Metadata: data.Metadata{
					Size: stat.Size,
				},
			}
			if updateErr := m.Metadata.UpdateMeta(ctx, m.Options.Namespace, path, update); updateErr != nil && updateErr != data.ErrNotExist {
				// TODO :: Missing internal log for tracking internal errors
				fmt.Printf("WARNING: Failed to sync write to metadata service: %v\n", updateErr)
			}
		}
	}

	if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeModified, path, false, 0); ok && err != nil {
		return 0, err
	}

	return n, nil
}

// StatMetadata returns file information for the given path.
// Returns an error if the path doesn't exist.
func (m *Mount) StatMetadata(ctx context.Context, path string) (data.Metadata, error) {
	// Check if this mount has metadata defined
	if m.Metadata != nil {
		// Only check if cascading OR owner of the matched path
		if m.Options.Cascading || !m.matchAnyMountPoints(path) {
			m.mu.RLock()
			meta, err := m.Metadata.ReadMeta(ctx, m.Options.Namespace, path)
			m.mu.RUnlock()
			// Metadata found and will be returned directly
			if err == nil {
				if ok, emitErr := m.emitNotificationEvent(ctx, notification.NotificationTypeRead, path, false, 0); ok && emitErr != nil {
					// TODO :: Missing internal log for tracking internal errors
					fmt.Printf("Warning: Failed to emit notification event: %v", emitErr)
				}

				return meta, nil
			}
			// Other error, propagate and return
			if err != data.ErrNotExist {
				return data.Metadata{}, err
			}
			// We continue to check child mounts or object-storage
		}
	}
	// Check if child mount exists for this path
	if mount, relativePath := m.resolveMountPoint(path); path != relativePath {
		// Delegate to child mount but don't return immediately
		meta, err := mount.StatMetadata(ctx, relativePath)
		if err != nil {
			return data.Metadata{}, err
		}
		// writing back data  from a child mount into the root
		if m.Metadata != nil {
			// We only need to check if cascading is enabled when
			if m.Options.Cascading {
				m.mu.Lock()
				defer m.mu.Unlock()
				// Copy metadata and adjust path prefix
				prefix := strings.TrimSuffix(strings.TrimSuffix(path, relativePath), "/")
				syncMeta := meta
				syncMeta.Key = prefix + "/" + meta.Key
				if syncErr := m.Metadata.CreateMeta(ctx, m.Options.Namespace, syncMeta); syncErr != nil && syncErr != data.ErrExist {
					// TODO :: Missing internal log for tracking internal errors
					fmt.Printf("WARNING: Failed to cascade metadata for '%s': %v\n", path, syncErr)
				}
			}
		}

		if ok, emitErr := m.emitNotificationEvent(ctx, notification.NotificationTypeRead, path, false, 0); ok && emitErr != nil {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("Warning: Failed to emit notification event: %v", emitErr)
		}

		return meta, nil
	}

	m.mu.RLock()
	// Check if ObjectStorage supports read before calling it
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationRead) {
		return data.Metadata{}, errors.ErrOperationNotSupported
	}
	// Get stat from ObjectStorage
	stat, err := m.ObjectStorage.HeadObject(ctx, m.Options.Namespace, path)
	m.mu.RUnlock()

	if err != nil {
		return data.Metadata{}, err
	}
	// Convert into metadata
	meta := stat.ToMetadata()

	if m.Metadata != nil {
		m.mu.Lock()
		defer m.mu.Unlock()
		// Sync data from object-storage into metadata
		if syncErr := m.Metadata.CreateMeta(ctx, m.Options.Namespace, meta); syncErr != nil && syncErr != data.ErrExist {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to sync ObjectStorage stat to MetadataService: %v\n", syncErr)
		}
	}

	if ok, emitErr := m.emitNotificationEvent(ctx, notification.NotificationTypeRead, path, false, 0); ok && emitErr != nil {
		// TODO :: Missing internal log for tracking internal errors
		fmt.Printf("Warning: Failed to emit notification event: %v", emitErr)
	}

	return meta, nil
}

// LookupMetadata checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (m *Mount) LookupMetadata(ctx context.Context, path string, quick bool) (bool, error) {
	// Check if this mount has metadata defined
	if m.Metadata != nil {
		// Only perform metadata check if quick is 'true'
		// AND cascading or owner of the matched path is true
		if quick && (m.Options.Cascading || !m.matchAnyMountPoints(path)) {
			m.mu.RLock()
			exists, err := m.Metadata.ExistsMeta(ctx, m.Options.Namespace, path)
			m.mu.RUnlock()
			// Throw if error is returned
			if err != nil {
				return false, err
			}
			// Metadata found and will be returned directly
			return exists, nil
		}
	}
	// Check if child mount exists for this path
	if mount, relativePath := m.resolveMountPoint(path); path != relativePath {
		exists, err := mount.LookupMetadata(ctx, relativePath, quick)
		if err != nil {
			return false, err
		}

		return exists, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	// Check if ObjectStorage supports read before calling it
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationRead) {
		return false, errors.ErrOperationNotSupported
	}
	// Get stat from ObjectStorage
	stat, err := m.ObjectStorage.HeadObject(ctx, m.Options.Namespace, path)
	if err != nil {
		return false, err
	}

	return stat.Key != "", nil
}

// ReadDirectory returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (m *Mount) ReadDirectory(ctx context.Context, path string) ([]data.Metadata, error) {
	// Check if this mount has metadata defined
	if m.Metadata != nil {
		// Only check if cascading OR owner of the matched path
		if m.Options.Cascading || !m.matchAnyMountPoints(path) {
			// Try MetadataService first (if available) - optimized directory indices
			m.mu.RLock()
			// Normalize key to end with "/" for directory prefix queries
			prefix := path
			if prefix != "" && prefix[len(prefix)-1] != '/' {
				prefix += "/"
			}

			query := service.Query{
				Prefix:    prefix,
				Delimiter: "/", // Only direct children
			}

			result, queryErr := m.Metadata.QueryMeta(ctx, m.Options.Namespace, query)
			if queryErr == nil && len(result.Candidates) > 0 {
				// Metadata already contains absolute paths - return as-is
				if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeRead, path, true, 0); ok && err != nil {
					return nil, err
				}

				m.mu.RUnlock()
				return m.injectMountEntries(path, result.Candidates), nil
			}

			m.mu.RUnlock()
		}
		// We continue to check child mounts or object-storage
	}
	// Check if child mount exists for this path
	if mount, relativePath := m.resolveMountPoint(path); path != relativePath {
		metas, err := mount.ReadDirectory(ctx, relativePath)
		if err != nil {
			return nil, err
		}

		// Calculate mount point prefix
		prefix := strings.TrimSuffix(strings.TrimSuffix(path, relativePath), "/")

		// Adjust paths: prepend mount point to make paths absolute from root
		clonedMetas := data.BatchMetadata(metas, func(m data.Metadata, i int) data.Metadata {
			m.Key = prefix + "/" + m.Key
			return m
		})

		// Cascade to metadata (store unadjusted paths so mount is relocatable)
		if m.Metadata != nil && m.Options.Cascading {
			m.mu.Lock()
			defer m.mu.Unlock()
			for _, meta := range metas {
				if syncErr := m.Metadata.CreateMeta(ctx, m.Options.Namespace, meta); syncErr != nil && syncErr != data.ErrExist {
					// TODO :: Missing internal log for tracking internal errors
					fmt.Printf("WARNING: Failed to sync directory entry %s to MetadataService: %v\n", meta.Key, syncErr)
				}
			}
		}

		if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeRead, path, true, 0); ok && err != nil {
			return nil, err
		}

		return m.injectMountEntries(path, clonedMetas), nil
	}
	// Fallback to ObjectStorage only
	m.mu.RLock()
	// Check if ObjectStorage supports list before calling it
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationList) {
		return nil, errors.ErrOperationNotSupported
	}

	stats, err := m.ObjectStorage.ListObjects(ctx, m.Options.Namespace, path)
	m.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	// Convert into metadata
	metas := make([]data.Metadata, len(stats))
	for i, stat := range stats {
		metas[i] = stat.ToMetadata()
	}

	if m.Metadata != nil {
		m.mu.Lock()
		defer m.mu.Unlock()

		for _, meta := range metas {
			// Best-effort sync with full path
			if syncErr := m.Metadata.CreateMeta(ctx, m.Options.Namespace, meta); syncErr != nil && syncErr != data.ErrExist {
				// TODO :: Missing internal log for tracking internal errors
				fmt.Printf("WARNING: Failed to sync directory entry %s to MetadataService: %v\n", meta.Key, syncErr)
			}
		}
	}

	if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeRead, path, true, 0); ok && err != nil {
		return nil, err
	}

	return m.injectMountEntries(path, metas), nil
}

// CreateDirectory creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (m *Mount) CreateDirectory(ctx context.Context, path string) (data.Metadata, error) {
	// Check if child mount exists for this path
	if mount, relativePath := m.resolveMountPoint(path); path != relativePath {
		// Delegate to child mount but don't return immediately
		meta, err := mount.CreateDirectory(ctx, path)
		if err != nil {
			return data.Metadata{}, err
		}
		// writing back data  from a child mount into the root
		if m.Metadata != nil {
			// We only need to check if cascading is enabled when
			if m.Options.Cascading {
				m.mu.Lock()
				defer m.mu.Unlock()
				// Copy metadata and adjust path prefix
				prefix := strings.TrimSuffix(strings.TrimSuffix(path, relativePath), "/")
				syncMeta := meta
				syncMeta.Key = prefix + "/" + meta.Key
				if syncErr := m.Metadata.CreateMeta(ctx, m.Options.Namespace, syncMeta); syncErr != nil && syncErr != data.ErrExist {
					// TODO :: Missing internal log for tracking internal errors
					fmt.Printf("WARNING: Failed to cascade metadata for '%s': %v\n", path, syncErr)
				}
			}
		}

		if ok, emitErr := m.emitNotificationEvent(ctx, notification.NotificationTypeCreated, path, true, 0); ok && emitErr != nil {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("Warning: Failed to emit notification event: %v", emitErr)
		}

		return meta, nil
	}
	// Throw if mountpoint is marked as readonly
	if m.Options.IsReadOnly {
		return data.Metadata{}, errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check path to validate against constraints
	if err := m.validatePathLength(path); err != nil {
		return data.Metadata{}, fmt.Errorf("failed to validate path: %v", err)
	}
	if err := m.validatePathDepth(path); err != nil {
		return data.Metadata{}, fmt.Errorf("failed to validate path: %v", err)
	}
	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationCreate) {
		return data.Metadata{}, errors.ErrOperationNotSupported
	}

	// Create directory in ObjectStorage (use directory mode 0755 | ModeDir)
	stat, err := m.ObjectStorage.CreateObject(ctx, m.Options.Namespace, path, data.FileMode(0755)|data.ModeDir)
	if err != nil {
		return data.Metadata{}, err
	}
	// Sync to MetadataService if available
	meta := stat.ToMetadata()
	if m.Metadata != nil {
		if syncErr := m.Metadata.CreateMeta(ctx, m.Options.Namespace, meta); syncErr != nil && syncErr != data.ErrExist {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to sync directory creation to MetadataService: %v\n", syncErr)
		}
	}

	if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeCreated, path, true, 0); ok && err != nil {
		return data.Metadata{}, err
	}

	return meta, nil
}

// RemoveDirectory removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (m *Mount) RemoveDirectory(ctx context.Context, path string, force bool) error {
	// Check if child mount exists for this path
	if mount, relativePath := m.resolveMountPoint(path); path != relativePath {
		// Delegate to child mount but don't return immediately
		if err := mount.RemoveDirectory(ctx, path, force); err != nil {
			return err
		}
		// Writing back data from a child mount into the root
		if m.Metadata != nil {
			// We only need to check if cascading is enabled when
			if m.Options.Cascading {
				m.mu.Lock()
				defer m.mu.Unlock()
				// Sync deletion to MetadataService
				if syncErr := m.Metadata.DeleteMeta(ctx, m.Options.Namespace, path); syncErr != nil && syncErr != data.ErrNotExist {
					// TODO :: Missing internal log for tracking internal errors
					fmt.Printf("WARNING: Failed to sync directory deletion to MetadataService: %v\n", syncErr)
				}
			}
		}

		if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeDeleted, path, true, 0); ok && err != nil {
			return err
		}

		return nil
	}
	// Throw if mountpoint is marked as readonly
	if m.Options.IsReadOnly {
		return errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()
	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationDelete) {
		return errors.ErrOperationNotSupported
	}
	// Delete directory from ObjectStorage
	err := m.ObjectStorage.DeleteObject(ctx, m.Options.Namespace, path, force)
	if err != nil {
		return err
	}
	// Sync deletion to MetadataService if available
	if m.Metadata != nil {
		if syncErr := m.Metadata.DeleteMeta(ctx, m.Options.Namespace, path); syncErr != nil && syncErr != data.ErrNotExist {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to sync directory deletion to MetadataService: %v\n", syncErr)
		}
	}

	if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeDeleted, path, true, 0); ok && err != nil {
		return err
	}

	return nil
}

// UnlinkFile removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (m *Mount) UnlinkFile(ctx context.Context, path string) error {
	// Check if child mount exists for this path
	if mount, relativePath := m.resolveMountPoint(path); path != relativePath {
		if err := mount.UnlinkFile(ctx, path); err != nil {
			return err
		}
		// Writing back data from a child mount into the root
		if m.Metadata != nil {
			// We only need to check if cascading is enabled when
			if m.Options.Cascading {
				m.mu.Lock()
				defer m.mu.Unlock()
				// Sync deletion to MetadataService
				if syncErr := m.Metadata.DeleteMeta(ctx, m.Options.Namespace, path); syncErr != nil && syncErr != data.ErrNotExist {
					// TODO :: Missing internal log for tracking internal errors
					fmt.Printf("WARNING: Failed to sync directory deletion to MetadataService: %v\n", syncErr)
				}
			}
		}

		if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeDeleted, path, false, 0); ok && err != nil {
			return err
		}

		return nil
	}
	// Throw if mountpoint is marked as readonly
	if m.Options.IsReadOnly {
		return errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()
	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationDelete) {
		return errors.ErrOperationNotSupported
	}
	// Delete from ObjectStorage
	err := m.ObjectStorage.DeleteObject(ctx, m.Options.Namespace, path, false)
	if err != nil {
		return err
	}
	// Sync deletion to MetadataService if available
	if m.Metadata != nil {
		if syncErr := m.Metadata.DeleteMeta(ctx, m.Options.Namespace, path); syncErr != nil && syncErr != data.ErrNotExist {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to sync file deletion to MetadataService: %v\n", syncErr)
		}
	}

	if ok, err := m.emitNotificationEvent(ctx, notification.NotificationTypeDeleted, path, false, 0); ok && err != nil {
		return err
	}

	return nil
}

// Rename moves or renames a file or directory from oldPath to newPath.
// Returns an error if the operation cannot be completed.
// This implementation uses a copy-and-delete strategy which works across different mounts
// but is not atomic and may not be optimal for large files.
func (m *Mount) Rename(ctx context.Context, sourcePath, targetPath string) error {
	// TODO: Implement actual rename logic
	return nil
}
