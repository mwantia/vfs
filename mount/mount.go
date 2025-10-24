package mount

import (
	"context"
	"strings"
	"sync"

	"github.com/mwantia/vfs/backend"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/extension/acl"
)

// VirtualMount represents a mounted filesystem.
// Implementations provide access to a specific storage backend.
// All paths passed to Mount methods are relative to the mount point.
type VirtualMount struct {
	mu   sync.RWMutex
	path string

	backends []backend.VirtualBackend
	options  *VirtualMountOptions
	storage  backend.VirtualObjectStorageBackend
}

func NewVirtualMount(path string, primary backend.VirtualObjectStorageBackend, opts ...VirtualMountOption) (*VirtualMount, error) {
	// Create default options
	options := NewDefaultOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	// Try to automatically identify if the primary backend has additional capabilities
	if options.Auto {
		capabilities := primary.GetCapabilities()

		if options.acl == nil && capabilities.Contains(backend.CapabilityACL) {
			options.acl = primary.(acl.VirtualAclBackend)
		}
		if options.cache == nil && capabilities.Contains(backend.CapabilityCache) {
			options.cache = primary.(backend.VirtualMetadataBackend)
		}
		if options.encrypt == nil && capabilities.Contains(backend.CapabilityEncrypt) {
			options.encrypt = primary.(backend.VirtualMetadataBackend)
		}
		if options.metadata == nil && capabilities.Contains(backend.CapabilityMetadata) {
			options.metadata = primary.(backend.VirtualMetadataBackend)
		}
		if options.multipart == nil && capabilities.Contains(backend.CapabilityMultipart) {
			options.multipart = primary.(backend.VirtualMetadataBackend)
		}
		if options.rubbish == nil && capabilities.Contains(backend.CapabilityRubbish) {
			options.rubbish = primary.(backend.VirtualMetadataBackend)
		}
		if options.snapshot == nil && capabilities.Contains(backend.CapabilitySnapshot) {
			options.snapshot = primary.(backend.VirtualMetadataBackend)
		}
		if options.versioning == nil && capabilities.Contains(backend.CapabilityVersioning) {
			options.versioning = primary.(backend.VirtualMetadataBackend)
		}
	}

	// Get unique backends to avoid calling Close() multiple times on same instance
	uniques := getUniqueBackends([]backend.VirtualBackend{
		primary,
		options.acl,
		options.cache,
		options.encrypt,
		options.metadata,
		options.multipart,
		options.rubbish,
		options.snapshot,
		options.versioning,
	})

	return &VirtualMount{
		path:     path,
		backends: uniques,
		options:  options,
		storage:  primary,
	}, nil
}

// Mount
func (vm *VirtualMount) Mount(ctx context.Context) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	errs := data.Errors{}

	for _, vb := range vm.backends {
		if err := vb.Open(ctx); err != nil {
			errs.Add(err)
		}
	}

	return errs.Errors()
}

// Unmount
func (vm *VirtualMount) Unmount(ctx context.Context) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	errs := data.Errors{}

	for _, vb := range vm.backends {
		if err := vb.Close(ctx); err != nil {
			errs.Add(err)
		}
	}

	return errs.Errors()
}

// Stat returns information about a virtual object.
// Returns ErrNotExist if the path doesn't exist.
func (vm *VirtualMount) Stat(ctx context.Context, path string) (*data.VirtualFileMetadata, error) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	// Load relative path by stripping the mountpoint as key
	key := vm.toRelativePath(path)
	// Try metadata first if available
	if vm.options.metadata != nil {
		meta, err := vm.options.metadata.ReadMeta(ctx, key)
		if err != nil {
			return nil, err
		}

		return meta, nil
	}
	// Fallback to storage to create metadata from object
	stat, err := vm.storage.HeadObject(ctx, key)
	if err != nil {
		return nil, err
	}

	meta := stat.ToMetadata()
	// Sync to metadata if available AND it's a separate backend instance
	if vm.options.metadata != nil && !vm.isStorageAlsoMetadata() {
		if err := vm.options.metadata.CreateMeta(ctx, meta); err != nil {
			return nil, err
		}
	}

	return meta, nil
}

// List returns all virtual objects under the given path.
// For directories, returns all direct children.
// For files, returns single entry with the file's info.
// Returns ErrNotExist if the path doesn't exist.
func (vm *VirtualMount) List(ctx context.Context, path string) ([]*data.VirtualFileMetadata, error) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	key := vm.toRelativePath(path)

	// Try metadata backend first if available
	if vm.options.metadata != nil {
		meta, err := vm.options.metadata.ReadMeta(ctx, key)
		if err != nil {
			return nil, err
		}

		// For non-directories, return single entry
		if !meta.Mode.IsDir() {
			return []*data.VirtualFileMetadata{meta}, nil
		}

		// For directories, we need to query all children
		// This is implementation-dependent, so we fall back to storage
	}

	// Use storage backend to list objects
	stats, err := vm.storage.ListObjects(ctx, key)
	if err != nil {
		return nil, err
	}

	// Convert stats to metadata
	result := make([]*data.VirtualFileMetadata, 0, len(stats))
	for _, stat := range stats {
		meta := stat.ToMetadata()

		// Sync to metadata if available AND it's a separate backend instance
		if vm.options.metadata != nil && !vm.isStorageAlsoMetadata() {
			if err := vm.options.metadata.CreateMeta(ctx, meta); err != nil {
				// Ignore errors for existing metadata
				continue
			}
		}

		result = append(result, meta)
	}

	return result, nil
}

// Read reads up to len(data) bytes from the object at path starting at offset.
// Returns the number of bytes read and any error encountered.
// Returns ErrNotExist if the path doesn't exist.
// Returns ErrIsDirectory if the path is a directory.
// If offset is beyond the file size, returns 0, io.EOF.
func (vm *VirtualMount) Read(ctx context.Context, path string, offset int64, dat []byte) (int, error) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	key := vm.toRelativePath(path)

	// Delegate to storage backend
	return vm.storage.ReadObject(ctx, key, offset, dat)
}

// Write writes data to the object at path starting at offset.
// If offset is beyond current file size, the gap is filled with zeros.
// Returns the number of bytes written and any error encountered.
// Returns ErrNotExist if the path doesn't exist (use Create first).
// Returns ErrIsDirectory if the path is a directory.
func (vm *VirtualMount) Write(ctx context.Context, path string, offset int64, dat []byte) (int, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	key := vm.toRelativePath(path)

	// Validate using metadata first if available
	if vm.options.metadata != nil {
		meta, err := vm.options.metadata.ReadMeta(ctx, key)
		if err != nil {
			// Metadata doesn't exist - try to populate from storage
			stat, statErr := vm.storage.HeadObject(ctx, key)
			if statErr != nil {
				return 0, statErr
			}
			// Create metadata from storage (only if separate backend)
			meta = stat.ToMetadata()
			if !vm.isStorageAlsoMetadata() {
				if createErr := vm.options.metadata.CreateMeta(ctx, meta); createErr != nil {
					return 0, createErr
				}
			}
		}

		// Validate it's not a directory
		if meta.Mode.IsDir() {
			return 0, data.ErrIsDirectory
		}
	}

	// Write to storage backend
	n, err := vm.storage.WriteObject(ctx, key, offset, dat)
	if err != nil {
		return n, err
	}

	// Sync metadata after successful write (only if separate backend)
	if vm.options.metadata != nil && !vm.isStorageAlsoMetadata() {
		writeEnd := offset + int64(n)
		// Get updated stat from storage to ensure accuracy
		stat, statErr := vm.storage.HeadObject(ctx, key)
		if statErr == nil {
			update := &data.VirtualFileMetadataUpdate{
				Mask: data.VirtualFileMetadataUpdateSize,
				Metadata: &data.VirtualFileMetadata{
					Size: stat.Size,
				},
			}
			// Update metadata (ModifyTime will be set automatically)
			vm.options.metadata.UpdateMeta(ctx, key, update)
		} else {
			// Fallback: calculate size from write
			update := &data.VirtualFileMetadataUpdate{
				Mask: data.VirtualFileMetadataUpdateSize,
				Metadata: &data.VirtualFileMetadata{
					Size: writeEnd,
				},
			}
			vm.options.metadata.UpdateMeta(ctx, key, update)
		}
	}

	return n, nil
}

// Create creates a new file or directory at the given path.
// For files, isDir should be false. For directories, isDir should be true.
// Returns ErrExist if the path already exists.
// Parent directories are NOT created automatically - they must exist.
func (vm *VirtualMount) Create(ctx context.Context, path string, fileMode data.VirtualFileMode) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	key := vm.toRelativePath(path)

	// Check if already exists in metadata
	if vm.options.metadata != nil {
		if exists, _ := vm.options.metadata.ExistsMeta(ctx, key); exists {
			return data.ErrExist
		}
	}

	// Create in storage backend
	stat, err := vm.storage.CreateObject(ctx, key, fileMode)
	if err != nil {
		return err
	}

	// Sync to metadata if available AND it's a separate backend instance
	if vm.options.metadata != nil && !vm.isStorageAlsoMetadata() {
		meta := stat.ToMetadata()
		if err := vm.options.metadata.CreateMeta(ctx, meta); err != nil {
			return err
		}
	}

	return nil
}

// Delete removes the object at the given path.
// If force is true and the object is a directory, removes all children recursively.
// If force is false and the directory is not empty, returns an error.
// Returns ErrNotExist if the path doesn't exist.
func (vm *VirtualMount) Delete(ctx context.Context, path string, force bool) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	key := vm.toRelativePath(path)

	// Validate existence in metadata if available
	if vm.options.metadata != nil {
		if exists, _ := vm.options.metadata.ExistsMeta(ctx, key); !exists {
			// Try storage as fallback
			if _, err := vm.storage.HeadObject(ctx, key); err != nil {
				return data.ErrNotExist
			}
		}
	}

	// Delete from storage backend
	if err := vm.storage.DeleteObject(ctx, key, force); err != nil {
		return err
	}

	// Sync deletion to metadata if available (only if separate backend)
	if vm.options.metadata != nil && !vm.isStorageAlsoMetadata() {
		// For directories with force=true, we need to delete all child metadata
		if force {
			// Get the metadata to check if it's a directory
			meta, err := vm.options.metadata.ReadMeta(ctx, key)
			if err == nil && meta.Mode.IsDir() {
				// Delete all child metadata entries
				// This is backend-specific, so we just delete the parent for now
				// The backend should handle cascading deletes
				vm.options.metadata.DeleteMeta(ctx, key)
			}
		} else {
			vm.options.metadata.DeleteMeta(ctx, key)
		}
	}

	return nil
}

// Truncate changes the size of the file at path.
// If the file is larger than size, the extra data is discarded.
// If the file is smaller than size, it is extended with zero bytes.
// Returns ErrNotExist if the path doesn't exist.
// Returns ErrIsDirectory if the path is a directory.
func (vm *VirtualMount) Truncate(ctx context.Context, path string, size int64) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	key := vm.toRelativePath(path)

	// Validate using metadata first if available
	if vm.options.metadata != nil {
		meta, err := vm.options.metadata.ReadMeta(ctx, key)
		if err != nil {
			// Metadata doesn't exist - try to populate from storage
			stat, statErr := vm.storage.HeadObject(ctx, key)
			if statErr != nil {
				return statErr
			}
			// Create metadata from storage (only if separate backend)
			meta = stat.ToMetadata()
			if !vm.isStorageAlsoMetadata() {
				if createErr := vm.options.metadata.CreateMeta(ctx, meta); createErr != nil {
					return createErr
				}
			}
		}

		// Validate it's not a directory
		if meta.Mode.IsDir() {
			return data.ErrIsDirectory
		}
	}

	// Truncate in storage backend
	if err := vm.storage.TruncateObject(ctx, key, size); err != nil {
		return err
	}

	// Sync metadata after successful truncate (only if separate backend)
	if vm.options.metadata != nil && !vm.isStorageAlsoMetadata() {
		update := &data.VirtualFileMetadataUpdate{
			Mask: data.VirtualFileMetadataUpdateSize,
			Metadata: &data.VirtualFileMetadata{
				Size: size,
			},
		}
		if err := vm.options.metadata.UpdateMeta(ctx, key, update); err != nil {
			// Log but continue - metadata sync is not critical
			return nil
		}
	}

	return nil
}

// toRelativePath removes the prefix from path.
// Returns the relative path after the prefix.
// It additionally removes any leading slashes.
func (vm *VirtualMount) toRelativePath(path string) string {
	if vm.path == "" {
		return path
	}

	if path == vm.path {
		return ""
	}

	relPath := strings.TrimPrefix(path, vm.path)
	return strings.TrimPrefix(relPath, "/")
}

// isStorageAlsoMetadata checks if the storage backend is also the metadata backend (same instance).
// Returns true if both capabilities are provided by the same backend instance.
func (vm *VirtualMount) isStorageAlsoMetadata() bool {
	if vm.options.metadata == nil {
		return false
	}
	storageAsMetadata, ok := vm.storage.(backend.VirtualMetadataBackend)
	return ok && storageAsMetadata == vm.options.metadata
}

// getUniqueBackends collects all backends and deduplicates them to ensure
// each unique backend instance is only returned once, even if assigned to
// multiple roles (e.g., storage backend also serving as metadata backend).
func getUniqueBackends(backends []backend.VirtualBackend) []backend.VirtualBackend {
	// Use map to track unique backend pointers
	seen := make(map[backend.VirtualBackend]struct{})
	// Helper to add backend if not nil and not already seen
	addBackend := func(b backend.VirtualBackend) {
		if b != nil {
			seen[b] = struct{}{}
		}
	}
	// Collect all backends
	for _, backend := range backends {
		addBackend(backend)
	}
	// Convert map keys to slice
	uniques := make([]backend.VirtualBackend, 0, len(seen))
	for b := range seen {
		uniques = append(uniques, b)
	}

	return uniques
}
