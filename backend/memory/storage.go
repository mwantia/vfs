package memory

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/mwantia/vfs/data"
)

func (mb *MemoryBackend) CreateObject(ctx context.Context, key string, mode data.VirtualFileMode) (*data.VirtualFileStat, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Check if path exists in B-tree
	if _, exists := mb.keys.Get(key); exists {
		return nil, data.ErrExist
	}

	// Verify parent directory exists
	parentKey := path.Dir(key)
	if parentKey != "." && parentKey != "" {
		parentID, exists := mb.keys.Get(parentKey)
		if !exists {
			return nil, data.ErrNotExist
		}

		parentMetadata, exists := mb.metadata[parentID]
		if !exists {
			return nil, data.ErrNotExist
		}

		if !parentMetadata.Mode.IsDir() {
			return nil, data.ErrNotDirectory
		}
	}

	meta := data.NewFileMetadata(key, 0, mode)
	stat := meta.ToStat()

	return stat, mb.CreateMeta(ctx, meta)
}

func (mb *MemoryBackend) ReadObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	meta, err := mb.ReadMeta(ctx, key)
	if err != nil {
		return 0, err
	}

	if meta.Mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	if offset >= meta.Size {
		return 0, io.EOF
	}

	buffer, exists := mb.datas[meta.ID]
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

func (mb *MemoryBackend) WriteObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	meta, err := mb.ReadMeta(ctx, key)
	if err != nil {
		return 0, err
	}

	if meta.Mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	writeEnd := offset + int64(len(dat))
	// Get existing buffer or create new one
	buffer, exists := mb.datas[meta.ID]
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
	mb.datas[meta.ID] = buffer
	if writeEnd > meta.Size {
		meta.Size = writeEnd
	}

	update := &data.VirtualFileMetadataUpdate{
		Mask:     data.VirtualFileMetadataUpdateSize,
		Metadata: meta,
	}

	if err := mb.UpdateMeta(ctx, key, update); err != nil {
		return 0, err
	}

	return len(dat), nil
}

func (mb *MemoryBackend) DeleteObject(ctx context.Context, key string, force bool) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	meta, err := mb.ReadMeta(ctx, key)
	if err != nil {
		return err
	}

	if meta.Mode.IsDir() {
		// Directories can only be deleted with force=true
		if !force {
			return data.ErrIsDirectory
		}

		// Build prefix for children lookup
		prefixKey := key
		if prefixKey != "" {
			prefixKey += "/"
		}

		// Collect all paths to delete (including this directory)
		var keysToDelete []string
		keysToDelete = append(keysToDelete, prefixKey)
		// Use B-tree range scan to find all children
		mb.keys.Scan(func(childPath string, _ string) bool {
			if strings.HasPrefix(childPath, prefixKey) {
				keysToDelete = append(keysToDelete, childPath)
			}
			// Continue scanning
			return true
		})

		errs := data.Errors{}

		// Delete all collected paths
		for _, delKey := range keysToDelete {
			if err := mb.DeleteMeta(ctx, delKey); err != nil {
				errs.Add(err)
			}
		}

		return errs.Errors()
	}

	return mb.DeleteMeta(ctx, key)
}

func (mb *MemoryBackend) ListObjects(ctx context.Context, key string) ([]*data.VirtualFileStat, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	meta, err := mb.ReadMeta(ctx, key)
	if err != nil {
		return nil, err
	}

	// For files, return single entry
	if !meta.Mode.IsDir() {
		return []*data.VirtualFileStat{
			meta.ToStat(),
		}, nil
	}

	// For directories, use B-tree range scan to find children
	prefixKey := key
	if prefixKey != "" {
		prefixKey += "/"
	}
	prefixLen := len(prefixKey)

	// Use map to deduplicate direct children
	children := make(map[string]*data.VirtualFileMetadata)
	// B-tree range scan: iterate over all paths starting with prefix
	mb.keys.Scan(func(childPath string, childID string) bool {
		// Skip the directory itself
		if childPath == key {
			return true
		}

		// Check if this path is under our directory
		if !strings.HasPrefix(childPath, prefixKey) {
			return true // Continue scanning (paths are ordered)
		}

		// Get relative path
		rel := childPath[prefixLen:]

		// Skip empty relative paths (shouldn't happen but be safe)
		if rel == "" {
			return true
		}

		// Check if this is a direct child (no slash in relative path)
		if slashIdx := strings.IndexByte(rel, '/'); slashIdx > 0 {
			// This is a nested child - only track the first segment
			childName := rel[:slashIdx]
			if _, seen := children[childName]; !seen {
				// Look up the directory metadata
				dirPath := prefixKey + childName
				dirMeta, err := mb.ReadMeta(ctx, dirPath)
				if err != nil {
					print(err) // TODO :: This needs further finetuning - Do we just allow failed lookups?
				} else {
					children[childName] = dirMeta
				}

			}
		} else {
			childMeta, err := mb.ReadMeta(ctx, childPath)
			if err != nil {
				print(err) // TODO :: This needs further finetuning - Do we just allow failed lookups?
			} else {
				children[rel] = childMeta
			}
		}
		// Continue scanning
		return true
	})

	// Convert map to slice
	result := make([]*data.VirtualFileStat, 0, len(children))
	for _, childMeta := range children {
		stat := childMeta.ToStat()
		result = append(result, stat)
	}

	return result, nil
}

func (mb *MemoryBackend) HeadObject(ctx context.Context, key string) (*data.VirtualFileStat, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	meta, err := mb.ReadMeta(ctx, key)
	if err != nil {
		return nil, err
	}

	return meta.ToStat(), nil
}

func (mb *MemoryBackend) TruncateObject(ctx context.Context, key string, size int64) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	meta, err := mb.ReadMeta(ctx, key)
	if err != nil {
		return err
	}

	if meta.Mode.IsDir() {
		return data.ErrIsDirectory
	}

	if size == meta.Size {
		return nil // No changes needed
	}

	buffer, exists := mb.datas[meta.ID]
	if exists {
		if size < meta.Size {
			// Shrink file
			mb.datas[meta.ID] = buffer[:size]
		} else {
			// Expand file with zeros
			newData := make([]byte, size)
			copy(newData, buffer)
			mb.datas[meta.ID] = newData
		}
	}

	meta.Size = size

	update := &data.VirtualFileMetadataUpdate{
		Mask:     data.VirtualFileMetadataUpdateSize,
		Metadata: meta,
	}

	return mb.UpdateMeta(ctx, key, update)
}
