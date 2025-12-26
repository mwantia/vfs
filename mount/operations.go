package mount

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/errors"
	"github.com/mwantia/vfs/mount/service"
)

// OpenFile opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (m *Mount) OpenFile(traversal context.TraversalContext, flags data.AccessMode) (MountStreamer, error) {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.OpenFile(mountTraversal, flags)
	}
	// Throw if mountpoint is marked as readonly
	if m.isReadonly() {
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
			update := &data.MetadataUpdate{
				Mask: data.MetadataUpdateSize,
				Metadata: &data.Metadata{
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

	return newTraversalMountStreamer(traversal, m, offset, flags), nil
}

// CloseFile closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (m *Mount) CloseFile(traversal context.TraversalContext, force bool) error {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.CloseFile(mountTraversal, force)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	return nil
}

// Read reads size bytes from the file at path starting at offset.
// Returns the data read or an error if the operation fails.
func (m *Mount) ReadFile(traversal context.TraversalContext, offset, size int64) ([]byte, error) {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.ReadFile(mountTraversal, offset, size)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.RLock()
	defer m.mu.RUnlock()

	namespace := m.Options.Namespace
	key := traversal.RelativePath()

	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationRead) {
		return nil, errors.ErrOperationNotSupported
	}

	// Allocate buffer for read
	buffer := make([]byte, size)

	// Read from ObjectStorage
	n, err := m.ObjectStorage.ReadObject(traversal, namespace, key, offset, buffer)
	if err != nil {
		return nil, err
	}

	// Return only the bytes actually read
	return buffer[:n], nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (m *Mount) WriteFile(traversal context.TraversalContext, offset int64, buffer []byte) (int, error) {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.WriteFile(mountTraversal, offset, buffer)
	}
	// Throw if mountpoint is marked as readonly
	if m.isReadonly() {
		return 0, errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	namespace := m.Options.Namespace
	key := traversal.RelativePath()

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
	n, err := m.ObjectStorage.WriteObject(traversal, namespace, key, offset, buffer)
	if err != nil {
		return 0, err
	}

	// Sync write to MetadataService if available (update size and modify time)
	if m.Metadata != nil {
		// Get current file size after write
		stat, statErr := m.ObjectStorage.HeadObject(traversal, namespace, key)
		if statErr != nil {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to get file stat after write for metadata sync: %v\n", statErr)
		} else {
			update := &data.MetadataUpdate{
				Mask: data.MetadataUpdateSize,
				Metadata: &data.Metadata{
					Size: stat.Size,
				},
			}
			if updateErr := m.Metadata.UpdateMeta(traversal, namespace, key, update); updateErr != nil && updateErr != data.ErrNotExist {
				// TODO :: Missing internal log for tracking internal errors
				fmt.Printf("WARNING: Failed to sync write to metadata service: %v\n", updateErr)
			}
		}
	}

	return n, nil
}

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (m *Mount) StatMetadata(traversal context.TraversalContext) (*data.Metadata, error) {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.StatMetadata(mountTraversal)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.RLock()
	defer m.mu.RUnlock()

	namespace := m.Options.Namespace
	key := traversal.RelativePath()

	// Try MetadataService first (if available)
	if m.Metadata != nil {
		meta, err := m.Metadata.ReadMeta(traversal, namespace, key)
		if err == nil {
			return meta, nil
		}
		// If not found in metadata, try to sync from ObjectStorage
		if err == data.ErrNotExist {
			// Check if ObjectStorage supports read before calling it
			caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
			if !caps.SupportsOperation(service.ObjectStorageOperationRead) {
				return nil, errors.ErrOperationNotSupported
			}

			// Try ObjectStorage
			stat, objErr := m.ObjectStorage.HeadObject(traversal, namespace, key)
			if objErr != nil {
				return nil, objErr
			}

			// Sync to MetadataService
			meta := stat.ToMetadata()
			if syncErr := m.Metadata.CreateMeta(traversal, namespace, meta); syncErr != nil && syncErr != data.ErrExist {
				// TODO :: Missing internal log for tracking internal errors
				fmt.Printf("WARNING: Failed to sync ObjectStorage stat to MetadataService: %v\n", syncErr)
			}
			return meta, nil
		}
		// Other errors from metadata service
		return nil, err
	}

	// Fallback to ObjectStorage only
	// Check if ObjectStorage supports read before calling it
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationRead) {
		return nil, errors.ErrOperationNotSupported
	}

	stat, err := m.ObjectStorage.HeadObject(traversal, namespace, key)
	if err != nil {
		return nil, err
	}

	return stat.ToMetadata(), nil
}

// Lookup checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (m *Mount) LookupMetadata(traversal context.TraversalContext) (bool, error) {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.LookupMetadata(mountTraversal)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.RLock()
	defer m.mu.RUnlock()

	// TODO: Implement actual lookup logic
	// When implemented, check ObjectStorage capabilities before calling ObjectStorage methods
	return false, nil
}

// ReadDirectory returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (m *Mount) ReadDirectory(traversal context.TraversalContext) ([]*data.Metadata, error) {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.ReadDirectory(mountTraversal)
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.RLock()
	defer m.mu.RUnlock()

	namespace := m.Options.Namespace
	key := traversal.RelativePath()

	// Try MetadataService first (if available) - optimized directory indices
	if m.Metadata != nil {
		// Normalize key to end with "/" for directory prefix queries
		prefix := key
		if prefix != "" && prefix[len(prefix)-1] != '/' {
			prefix += "/"
		}

		query := &service.Query{
			Prefix:    prefix,
			Delimiter: "/", // Only direct children
		}

		result, queryErr := m.Metadata.QueryMeta(traversal, namespace, query)
		if queryErr == nil && len(result.Candidates) > 0 {
			// Strip prefix from metadata keys to match ListObjects behavior
			// Metadata stores full paths, but ReadDirectory should return basenames
			if key != "" {
				for _, meta := range result.Candidates {
					meta.Key = strings.TrimPrefix(meta.Key, key+"/")
				}
			}
			return m.injectMountEntries(key, result.Candidates), nil
		}

		// If metadata is empty or error, sync from ObjectStorage
		// Check if ObjectStorage supports list before calling it
		caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
		if !caps.SupportsOperation(service.ObjectStorageOperationList) {
			return nil, errors.ErrOperationNotSupported
		}

		stats, objErr := m.ObjectStorage.ListObjects(traversal, namespace, key)
		if objErr != nil {
			return nil, objErr
		}
		// Convert to metadata list (keep basenames for return value)
		metaList := make([]*data.Metadata, len(stats))
		for i, stat := range stats {
			meta := stat.ToMetadata()
			metaList[i] = meta
			// Clone and fix key for metadata storage
			// ListObjects returns keys relative to the listed directory (just basenames)
			// but metadata needs full paths relative to mount root
			clone := meta.Clone()
			if key != "" && !strings.Contains(clone.Key, "/") {
				clone.Key = key + "/" + clone.Key
			}
			// Best-effort sync with full path
			if syncErr := m.Metadata.CreateMeta(traversal, namespace, clone); syncErr != nil && syncErr != data.ErrExist {
				// TODO :: Missing internal log for tracking internal errors
				fmt.Printf("WARNING: Failed to sync directory entry %s to MetadataService: %v\n", clone.Key, syncErr)
			}
		}

		return m.injectMountEntries(key, metaList), nil
	}

	// Fallback to ObjectStorage only
	// Check if ObjectStorage supports list before calling it
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationList) {
		return nil, errors.ErrOperationNotSupported
	}

	stats, err := m.ObjectStorage.ListObjects(traversal, namespace, key)
	if err != nil {
		return nil, err
	}

	result := make([]*data.Metadata, len(stats))
	for i, stat := range stats {
		result[i] = stat.ToMetadata()
	}

	return m.injectMountEntries(key, result), nil
}

// findDirectChildMounts returns mount entries that are direct children of the given path.
// For example, if path is "a" and fstab contains ["a/b/c", "a/d", "x/y"]:
//   - Returns: ["b", "d"] (direct children only)
//   - Excludes: "b/c" (nested under b), "x/y" (different parent)
//
// Thread-safety: Caller must hold m.mu (RLock or Lock).
func (m *Mount) findDirectChildMounts(path string) []string {
	// Normalize path for comparison
	normalizedPath := strings.Trim(path, "/")

	// Root path special case
	var searchPrefix string
	if normalizedPath == "" {
		searchPrefix = "" // Root - no prefix needed
	} else {
		searchPrefix = normalizedPath + "/"
	}

	childNames := make(map[string]bool)

	for mountPath := range m.fstab {
		normalizedMount := strings.Trim(mountPath, "/")

		// Root path: show top-level mounts (no "/" in path)
		if normalizedPath == "" {
			// Split mount path and take first segment
			parts := strings.SplitN(normalizedMount, "/", 2)
			if parts[0] != "" {
				childNames[parts[0]] = true
			}
			continue
		}

		// Non-root: check prefix match
		if !strings.HasPrefix(normalizedMount, searchPrefix) {
			continue // Not under this path
		}

		// Get relative path after prefix
		relativePath := strings.TrimPrefix(normalizedMount, searchPrefix)

		// Only direct children (no nested slashes)
		parts := strings.SplitN(relativePath, "/", 2)
		if parts[0] != "" {
			childNames[parts[0]] = true
		}
	}

	// Convert map to sorted slice for deterministic output
	result := make([]string, 0, len(childNames))
	for name := range childNames {
		result = append(result, name)
	}

	// Sort alphabetically
	sort.Strings(result)

	return result
}

// createMountMetadata generates synthetic metadata for a mount entry.
// The metadata is virtual (not persisted) and uses mount creation time.
func (m *Mount) createMountMetadata(name string, key string) *data.Metadata {
	now := m.createTime // Use mount creation time for consistency

	return &data.Metadata{
		ID:          uuid.Must(uuid.NewV7()).String(), // Generate unique ID
		Key:         key,
		Mode:        data.ModeMount | data.ModeDir | 0555, // Mount + Directory + r-xr-xr-x
		Size:        0,                                    // Directories have 0 size
		AccessTime:  now,
		ModifyTime:  now,
		CreateTime:  now,
		ContentType: "", // Directories have no content type
		Attributes: map[string]string{
			"mount.virtual": "true", // Mark as virtual mount entry
		},
	}
}

// injectMountEntries adds mount entries to directory listing and handles path conflicts.
// Mount entries shadow any real directories with the same name (mount takes precedence).
//
// Thread-safety: Caller must hold m.mu (at least RLock).
func (m *Mount) injectMountEntries(dirPath string, entries []*data.Metadata) []*data.Metadata {
	// Find direct child mounts for this path
	childMounts := m.findDirectChildMounts(dirPath)

	if len(childMounts) == 0 {
		return entries // No mounts to inject
	}

	// Create mount metadata entries
	mountEntries := make(map[string]*data.Metadata, len(childMounts))
	for _, mountName := range childMounts {
		// Build full key for metadata
		var fullKey string
		if dirPath == "" || dirPath == "/" {
			fullKey = mountName
		} else {
			fullKey = dirPath + "/" + mountName
		}

		mountEntries[mountName] = m.createMountMetadata(mountName, fullKey)
	}

	// Deduplicate: remove real entries that conflict with mounts (mount shadows)
	filtered := make([]*data.Metadata, 0, len(entries))
	for _, entry := range entries {
		// Extract base name from entry key
		baseName := entry.Key
		if idx := strings.LastIndex(entry.Key, "/"); idx >= 0 {
			baseName = entry.Key[idx+1:]
		}

		// Skip if mount exists with same name (mount shadows directory)
		if _, isMountPoint := mountEntries[baseName]; !isMountPoint {
			filtered = append(filtered, entry)
		}
	}

	// Combine filtered entries + mount entries
	result := make([]*data.Metadata, 0, len(filtered)+len(mountEntries))
	result = append(result, filtered...)
	for _, mountMeta := range mountEntries {
		result = append(result, mountMeta)
	}

	// Sort by key for deterministic output
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

// CreateDirectory creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (m *Mount) CreateDirectory(traversal context.TraversalContext) error {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.CreateDirectory(mountTraversal)
	}
	// Throw if mountpoint is marked as readonly
	if m.isReadonly() {
		return errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	namespace := m.Options.Namespace
	key := traversal.RelativePath()

	// Check path constraints
	if err := m.validatePathLength(key); err != nil {
		return err
	}
	if err := m.validatePathDepth(key); err != nil {
		return err
	}

	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationCreate) {
		return errors.ErrOperationNotSupported
	}

	// Create directory in ObjectStorage (use directory mode 0755 | ModeDir)
	stat, err := m.ObjectStorage.CreateObject(traversal, namespace, key, data.FileMode(0755)|data.ModeDir)
	if err != nil {
		return err
	}

	// Sync to MetadataService if available
	if m.Metadata != nil {
		meta := stat.ToMetadata()
		if syncErr := m.Metadata.CreateMeta(traversal, namespace, meta); syncErr != nil && syncErr != data.ErrExist {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to sync directory creation to MetadataService: %v\n", syncErr)
		}
	}

	return nil
}

// RemoveDirectory removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (m *Mount) RemoveDirectory(traversal context.TraversalContext, force bool) error {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.RemoveDirectory(mountTraversal, force)
	}
	// Throw if mountpoint is marked as readonly
	if m.isReadonly() {
		return errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	namespace := m.Options.Namespace
	key := traversal.RelativePath()

	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationDelete) {
		return errors.ErrOperationNotSupported
	}

	// Delete directory from ObjectStorage
	err := m.ObjectStorage.DeleteObject(traversal, namespace, key, force)
	if err != nil {
		return err
	}

	// Sync deletion to MetadataService if available
	if m.Metadata != nil {
		if syncErr := m.Metadata.DeleteMeta(traversal, namespace, key); syncErr != nil && syncErr != data.ErrNotExist {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to sync directory deletion to MetadataService: %v\n", syncErr)
		}
	}

	return nil
}

// UnlinkFile removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (m *Mount) UnlinkFile(traversal context.TraversalContext) error {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.UnlinkFile(mountTraversal)
	}
	// Throw if mountpoint is marked as readonly
	if m.isReadonly() {
		return errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	namespace := m.Options.Namespace
	key := traversal.RelativePath()

	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationDelete) {
		return errors.ErrOperationNotSupported
	}

	// Delete from ObjectStorage
	err := m.ObjectStorage.DeleteObject(traversal, namespace, key, false)
	if err != nil {
		return err
	}

	// Sync deletion to MetadataService if available
	if m.Metadata != nil {
		if syncErr := m.Metadata.DeleteMeta(traversal, namespace, key); syncErr != nil && syncErr != data.ErrNotExist {
			// TODO :: Missing internal log for tracking internal errors
			fmt.Printf("WARNING: Failed to sync file deletion to MetadataService: %v\n", syncErr)
		}
	}

	return nil
}

// Rename moves or renames a file or directory from oldPath to newPath.
// Returns an error if the operation cannot be completed.
// This implementation uses a copy-and-delete strategy which works across different mounts
// but is not atomic and may not be optimal for large files.
func (m *Mount) Rename(traversal context.TraversalContext, path string) error {
	//
	if match, mount, mountTraversal := m.resolve(traversal); match {
		return mount.Rename(mountTraversal, path)
	}
	// Throw if mountpoint is marked as readonly
	if m.isReadonly() {
		return errors.ErrMountIsReadonly
	}
	// Only lock after checks for traversing submounts is done.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check supported service operations
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	if !caps.SupportsOperation(service.ObjectStorageOperationWrite) {
		return errors.ErrOperationNotSupported
	}

	// TODO: Implement actual rename logic
	return nil
}

// validatePathLength checks if the path exceeds the maximum path length constraint
func (m *Mount) validatePathLength(path string) error {
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	maxLength := caps.ObjectStorage.Constraints.MaxPathLength
	if maxLength > 0 && len(path) > maxLength {
		return fmt.Errorf("%w: path length %d exceeds maximum %d", errors.ErrPathTooLong, len(path), maxLength)
	}
	return nil
}

// validatePathDepth checks if the path exceeds the maximum depth constraint
func (m *Mount) validatePathDepth(path string) error {
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	maxDepth := caps.ObjectStorage.Constraints.MaxPathDepth
	if maxDepth > 0 {
		// Count directory separators to determine depth
		// Empty path = depth 0, "a" = depth 1, "a/b" = depth 2, etc.
		depth := 0
		if path != "" {
			depth = 1
			for _, char := range path {
				if char == '/' {
					depth++
				}
			}
		}
		if depth > maxDepth {
			return fmt.Errorf("%w: path depth %d exceeds maximum %d", errors.ErrPathTooDeep, depth, maxDepth)
		}
	}
	return nil
}

// validateObjectSize checks if the object size violates size constraints
func (m *Mount) validateObjectSize(size int64) error {
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	constraints := caps.ObjectStorage.Constraints

	if constraints.MinObjectSize > 0 && size < constraints.MinObjectSize {
		return fmt.Errorf("%w: object size %d is below minimum %d", errors.ErrObjectTooSmall, size, constraints.MinObjectSize)
	}

	if constraints.MaxObjectSize > 0 && size > constraints.MaxObjectSize {
		return fmt.Errorf("%w: object size %d exceeds maximum %d", errors.ErrObjectTooLarge, size, constraints.MaxObjectSize)
	}

	return nil
}
