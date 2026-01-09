package ephemeral

import (
	"context"
	"io"
	"path"
	"strings"
	"time"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

func (s *EphemeralObjectStorageService) GetLifecycle() service.Lifecycle {
	return s.driver
}

func (eos *EphemeralObjectStorageService) CreateObject(ctx context.Context, key string, mode data.FileMode) (data.FileStat, error) {
	eos.driver.mu.Lock()
	defer eos.driver.mu.Unlock()

	// Check if object already exists
	named := key
	if _, exists := eos.driver.stats[named]; exists {
		return data.FileStat{}, data.ErrExist
	}

	// Verify parent directory exists (except for root)
	parentKey := path.Dir(key)
	if parentKey != "." && parentKey != "" && parentKey != "/" {
		parentNamed := parentKey
		parentStat, exists := eos.driver.stats[parentNamed]
		if !exists {
			return data.FileStat{}, data.ErrNotExist
		}

		if !parentStat.Mode.IsDir() {
			return data.FileStat{}, data.ErrNotDirectory
		}
	}

	// Create new FileStat
	now := time.Now()
	stat := data.FileStat{
		Key:        key,
		Mode:       mode,
		Size:       0,
		CreateTime: now,
		ModifyTime: now,
	}

	// Set content type based on what we're creating
	if !mode.IsDir() {
		// For files, detect content type from extension
		stat.ContentType = data.GetMIMEType(key)
	}
	// For directories, leave ContentType empty

	// Store in maps
	eos.driver.stats[named] = stat

	// Initialize empty data buffer for files
	if !mode.IsDir() {
		eos.driver.datas[named] = make([]byte, 0)
	}

	return stat, nil
}

func (eos *EphemeralObjectStorageService) ReadObject(ctx context.Context, key string, offset int64, buffer []byte) (int, error) {
	eos.driver.mu.RLock()
	defer eos.driver.mu.RUnlock()

	// Get object stat
	named := key
	stat, exists := eos.driver.stats[named]
	if !exists {
		return 0, data.ErrNotExist
	}

	if stat.Mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	// Check offset
	if offset >= stat.Size {
		return 0, io.EOF
	}

	// Get data buffer
	objData, exists := eos.driver.datas[named]
	if !exists || len(objData) == 0 {
		return 0, nil
	}

	// Calculate how many bytes we can actually read
	available := stat.Size - offset
	toRead := min(int64(len(buffer)), available)

	// Copy data from object buffer to provided buffer
	n := copy(buffer, objData[offset:offset+toRead])
	return n, nil
}

func (eos *EphemeralObjectStorageService) WriteObject(ctx context.Context, key string, offset int64, buffer []byte) (int, error) {
	eos.driver.mu.Lock()
	defer eos.driver.mu.Unlock()

	// Get object stat
	named := key
	stat, exists := eos.driver.stats[named]
	if !exists {
		return 0, data.ErrNotExist
	}

	if stat.Mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	writeEnd := offset + int64(len(buffer))

	// Get existing buffer or create new one
	objData, exists := eos.driver.datas[named]
	if !exists {
		objData = make([]byte, 0)
	}

	// Determine the new required size
	newSize := max(writeEnd, stat.Size)

	// Expand buffer if needed
	if int64(len(objData)) < newSize {
		newBuffer := make([]byte, newSize)
		copy(newBuffer, objData)
		objData = newBuffer
	}

	// Write the data
	copy(objData[offset:], buffer)

	// Update storage and stat
	eos.driver.datas[named] = objData
	if writeEnd > stat.Size {
		stat.Size = writeEnd
	}
	stat.ModifyTime = time.Now()
	eos.driver.stats[named] = stat

	return len(buffer), nil
}

func (eos *EphemeralObjectStorageService) DeleteObject(ctx context.Context, key string, force bool) error {
	eos.driver.mu.Lock()
	defer eos.driver.mu.Unlock()

	// Get object stat
	named := key
	stat, exists := eos.driver.stats[named]
	if !exists {
		return data.ErrNotExist
	}

	if stat.Mode.IsDir() {
		// Directories REQUIRE force=true to delete (even if empty)
		if !force {
			return data.ErrIsDirectory
		}

		// Force delete - collect and delete all children
		prefix := named
		if key != "" {
			prefix += "/"
		}

		var keysToDelete []string
		for childKey := range eos.driver.stats {
			if strings.HasPrefix(childKey, prefix) {
				keysToDelete = append(keysToDelete, childKey)
			}
		}

		// Delete all children
		for _, delKey := range keysToDelete {
			delete(eos.driver.stats, delKey)
			delete(eos.driver.datas, delKey)
		}
	}

	// Delete the object itself
	delete(eos.driver.stats, named)
	delete(eos.driver.datas, named)

	return nil
}

func (eos *EphemeralObjectStorageService) ListObjects(ctx context.Context, key string) ([]data.FileStat, error) {
	eos.driver.mu.RLock()
	defer eos.driver.mu.RUnlock()

	// For non-root paths, verify the directory exists
	if key != "" {
		named := key
		stat, exists := eos.driver.stats[named]
		if !exists {
			return nil, data.ErrNotExist
		}

		// If it's a file, return single entry
		if !stat.Mode.IsDir() {
			return []data.FileStat{stat}, nil
		}
	}

	// List directory contents (direct children only)
	prefix := key
	if key != "" {
		prefix += "/"
	}
	prefixLen := len(prefix)

	// Use map to deduplicate direct children
	children := make(map[string]data.FileStat)

	for childKey, stat := range eos.driver.stats {
		// Check if this key belongs to our directory
		if !strings.HasPrefix(childKey, prefix) {
			continue
		}

		// Get relative path (without prefix)
		rel := childKey[prefixLen:]

		// Skip empty relative paths
		if rel == "" {
			continue
		}

		// Check if this is a direct child (no slash in relative path)
		if slashIdx := strings.IndexByte(rel, '/'); slashIdx > 0 {
			// This is a nested child - only track the first segment as a directory
			childName := rel[:slashIdx]
			if _, seen := children[childName]; !seen {
				// Look up the directory stat
				dirKey := key
				if dirKey != "" {
					dirKey += "/"
				}
				dirKey += childName

				dirNamed := dirKey
				if dirStat, exists := eos.driver.stats[dirNamed]; exists {
					children[childName] = dirStat
				}
			}
		} else {
			// Direct child - add its stat
			children[rel] = stat
		}
	}

	// Convert map to slice
	result := make([]data.FileStat, 0, len(children))
	for _, stat := range children {
		result = append(result, stat)
	}

	return result, nil
}

func (eos *EphemeralObjectStorageService) HeadObject(ctx context.Context, key string) (data.FileStat, error) {
	eos.driver.mu.RLock()
	defer eos.driver.mu.RUnlock()

	// Get object stat
	named := key
	stat, exists := eos.driver.stats[named]
	if !exists {
		return data.FileStat{}, data.ErrNotExist
	}

	return stat, nil
}

func (eos *EphemeralObjectStorageService) TruncateObject(ctx context.Context, key string, size int64) error {
	eos.driver.mu.Lock()
	defer eos.driver.mu.Unlock()

	// Get object stat
	named := key
	stat, exists := eos.driver.stats[named]
	if !exists {
		return data.ErrNotExist
	}

	if stat.Mode.IsDir() {
		return data.ErrIsDirectory
	}

	if size == stat.Size {
		return nil // No changes needed
	}

	buffer, exists := eos.driver.datas[named]
	if exists {
		if size < stat.Size {
			// Shrink file
			eos.driver.datas[named] = buffer[:size]
		} else {
			// Expand file with zeros
			newData := make([]byte, size)
			copy(newData, buffer)
			eos.driver.datas[named] = newData
		}
	} else {
		// No data exists, create buffer of specified size
		eos.driver.datas[named] = make([]byte, size)
	}

	stat.Size = size
	stat.ModifyTime = time.Now()
	eos.driver.stats[named] = stat

	return nil
}
