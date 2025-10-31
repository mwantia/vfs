package ephemeral

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
	"github.com/mwantia/vfs/mount/backend"
)

func (eb *EphemeralBackend) CreateObject(ctx context.Context, namespace, key string, mode data.FileMode) (*data.FileStat, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	// Check if path exists in B-tree
	nsKey := backend.NamespacedKey(namespace, key)
	if _, exists := eb.keys.Get(nsKey); exists {
		return nil, data.ErrExist
	}

	// Verify parent directory exists
	parentKey := path.Dir(key)
	if parentKey != "." && parentKey != "" {
		parentMeta, err := eb.readMetaUnsafe(ctx, namespace, parentKey)
		if err != nil {
			return nil, data.ErrNotExist
		}

		if !parentMeta.Mode.IsDir() {
			return nil, data.ErrNotDirectory
		}
	}

	meta := data.NewFileMetadata(key, 0, mode)
	stat := meta.ToStat()

	return stat, eb.createMetaUnsafe(ctx, namespace, meta)
}

func (eb *EphemeralBackend) ReadObject(ctx context.Context, namespace, key string, offset int64, dat []byte) (int, error) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	meta, err := eb.readMetaUnsafe(ctx, namespace, key)
	if err != nil {
		return 0, err
	}

	if meta.Mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	if offset >= meta.Size {
		return 0, io.EOF
	}

	buffer, exists := eb.datas[meta.ID]
	if !exists {
		return 0, nil
	}

	// Calculate how many bytes we can actually read
	available := meta.Size - offset
	toRead := min(int64(len(dat)), available)
	// Copy data from file buffer
	n := copy(dat, buffer[offset:offset+toRead])
	return n, nil
}

func (eb *EphemeralBackend) WriteObject(ctx context.Context, namespace, key string, offset int64, dat []byte) (int, error) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	meta, err := eb.readMetaUnsafe(ctx, namespace, key)
	if err != nil {
		return 0, err
	}

	if meta.Mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	writeEnd := offset + int64(len(dat))
	// Get existing buffer or create new one
	buffer, exists := eb.datas[meta.ID]
	if !exists {
		buffer = make([]byte, 0)
	}

	// Determine the new required size
	newSize := max(writeEnd, meta.Size)

	// Expand buffer if needed
	if int64(len(buffer)) < newSize {
		newBuffer := make([]byte, newSize)
		copy(newBuffer, buffer)
		buffer = newBuffer
	}

	// Write the data
	copy(buffer[offset:], dat)

	// Update storage
	eb.datas[meta.ID] = buffer
	if writeEnd > meta.Size {
		meta.Size = writeEnd
	}

	update := &data.MetadataUpdate{
		Mask:     data.MetadataUpdateSize,
		Metadata: meta,
	}

	if err := eb.updateMetaUnsafe(ctx, namespace, key, update); err != nil {
		return 0, err
	}

	return len(dat), nil
}

func (eb *EphemeralBackend) DeleteObject(ctx context.Context, namespace, key string, force bool) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	meta, err := eb.readMetaUnsafe(ctx, namespace, key)
	if err != nil {
		return err
	}

	if meta.Mode.IsDir() {
		// Directories can only be deleted with force=true
		if !force {
			return data.ErrIsDirectory
		}

		// Build prefix for children lookup (using namespaced keys)
		nsPrefixKey := backend.NamespacedKey(namespace, key)
		if key != "" {
			nsPrefixKey += "/"
		}

		// Collect all paths to delete (including this directory)
		var keysToDelete []string
		keysToDelete = append(keysToDelete, key)
		// Use B-tree range scan to find all children
		eb.keys.Scan(func(nsChildPath string, _ string) bool {
			if strings.HasPrefix(nsChildPath, nsPrefixKey) {
				// Strip namespace to get actual key
				childKey := strings.TrimPrefix(nsChildPath, namespace+":")
				keysToDelete = append(keysToDelete, childKey)
			}
			// Continue scanning
			return true
		})

		errs := errors.Errors{}

		// Delete all collected paths
		for _, delKey := range keysToDelete {
			if err := eb.deleteMetaUnsafe(ctx, namespace, delKey); err != nil {
				errs.Add(err)
			}
		}

		return errs.Errors()
	}

	return eb.deleteMetaUnsafe(ctx, namespace, key)
}

func (eb *EphemeralBackend) ListObjects(ctx context.Context, namespace, key string) ([]*data.FileStat, error) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	// For root directory, skip the existence check - root is implicit
	if key != "" {
		meta, err := eb.readMetaUnsafe(ctx, namespace, key)
		if err != nil {
			return nil, err
		}

		// For files, return single entry
		if !meta.Mode.IsDir() {
			return []*data.FileStat{
				meta.ToStat(),
			}, nil
		}
	}

	// For directories, use B-tree range scan to find children (using namespaced keys)
	nsPrefixKey := backend.NamespacedKey(namespace, key)
	if key != "" {
		nsPrefixKey += "/"
	}
	nsPrefixLen := len(nsPrefixKey)

	// Use map to deduplicate direct children
	children := make(map[string]*data.Metadata)
	// B-tree range scan: iterate over all paths starting with prefix
	eb.keys.Scan(func(nsChildPath string, childID string) bool {
		// Check if this path is under our namespace and directory
		if !strings.HasPrefix(nsChildPath, nsPrefixKey) {
			return true // Continue scanning (paths are ordered)
		}

		// Get relative path (without namespace prefix)
		rel := nsChildPath[nsPrefixLen:]

		// Skip empty relative paths (shouldn't happen but be safe)
		if rel == "" {
			return true
		}

		// Strip namespace to get the actual child key
		childKey := strings.TrimPrefix(nsChildPath, namespace+":")

		// Check if this is a direct child (no slash in relative path)
		if slashIdx := strings.IndexByte(rel, '/'); slashIdx > 0 {
			// This is a nested child - only track the first segment
			childName := rel[:slashIdx]
			if _, seen := children[childName]; !seen {
				// Look up the directory metadata
				dirKey := key
				if dirKey != "" {
					dirKey += "/"
				}
				dirKey += childName
				dirMeta, err := eb.readMetaUnsafe(ctx, namespace, dirKey)
				if err == nil {
					children[childName] = dirMeta
				}
			}
		} else {
			childMeta, err := eb.readMetaUnsafe(ctx, namespace, childKey)
			if err == nil {
				children[rel] = childMeta
			}
		}
		// Continue scanning
		return true
	})

	// Convert map to slice
	result := make([]*data.FileStat, 0, len(children))
	for _, childMeta := range children {
		stat := childMeta.ToStat()
		result = append(result, stat)
	}

	return result, nil
}

func (eb *EphemeralBackend) HeadObject(ctx context.Context, namespace, key string) (*data.FileStat, error) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	meta, err := eb.readMetaUnsafe(ctx, namespace, key)
	if err != nil {
		return nil, err
	}

	return meta.ToStat(), nil
}

func (eb *EphemeralBackend) TruncateObject(ctx context.Context, namespace, key string, size int64) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	meta, err := eb.readMetaUnsafe(ctx, namespace, key)
	if err != nil {
		return err
	}

	if meta.Mode.IsDir() {
		return data.ErrIsDirectory
	}

	if size == meta.Size {
		return nil // No changes needed
	}

	buffer, exists := eb.datas[meta.ID]
	if exists {
		if size < meta.Size {
			// Shrink file
			eb.datas[meta.ID] = buffer[:size]
		} else {
			// Expand file with zeros
			newData := make([]byte, size)
			copy(newData, buffer)
			eb.datas[meta.ID] = newData
		}
	}

	meta.Size = size

	update := &data.MetadataUpdate{
		Mask:     data.MetadataUpdateSize,
		Metadata: meta,
	}

	return eb.updateMetaUnsafe(ctx, namespace, key, update)
}
