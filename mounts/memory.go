package mounts

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mwantia/vfs"
)

// MemoryMount is a thread-safe in-memory filesystem implementation.
// All files and directories are stored in RAM and are lost when the mount is destroyed.
// This implementation is fully CRUD-compliant and supports metadata storage.
type MemoryMount struct {
	mu    sync.RWMutex           // Protects files map
	files map[string]*MemoryFile // Maps paths to file entries
}

// MemoryFile represents a single file or directory in memory.
type MemoryFile struct {
	data     []byte            // File content (empty for directories)
	size     int64             // Size in bytes
	isDir    bool              // Whether this is a directory
	modTime  time.Time         // Last modification time
	metadata map[string]string // Extended metadata
}

// NewMemory creates a new in-memory filesystem with an empty root directory.
func NewMemory() *MemoryMount {
	memory := &MemoryMount{
		files: make(map[string]*MemoryFile),
	}
	memory.files[""] = &MemoryFile{
		data:     make([]byte, 0),
		size:     0,
		isDir:    true,
		modTime:  time.Now(),
		metadata: make(map[string]string),
	}

	return memory
}

// GetCapabilities returns the capabilities supported by this mount.
// MemoryMount supports CRUD operations and metadata storage.
func (m *MemoryMount) GetCapabilities() vfs.VirtualMountCapabilities {
	return vfs.VirtualMountCapabilities{
		Capabilities: []vfs.VirtualMountCapability{
			vfs.VirtualMountCapabilityCRUD,
			vfs.VirtualMountCapabilityMetadata,
		},
	}
}

// Create creates a new file or directory at the given path.
// Returns ErrExist if the path already exists.
// Parent directories must exist - they are NOT created automatically.
func (m *MemoryMount) Create(ctx context.Context, p string, isDir bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.files[p]; exists {
		return vfs.ErrExist
	}

	// Verify parent directory exists
	parent := path.Dir(p)
	if parent != "." && parent != "" {
		parentFile, exists := m.files[parent]
		if !exists {
			return vfs.ErrNotExist
		}
		if !parentFile.isDir {
			return vfs.ErrNotDirectory
		}
	}

	m.files[p] = &MemoryFile{
		data:     make([]byte, 0),
		size:     0,
		isDir:    isDir,
		modTime:  time.Now(),
		metadata: make(map[string]string),
	}

	return nil
}

// Read reads up to len(data) bytes from the file at path starting at offset.
// Returns the number of bytes read and any error encountered.
// Returns ErrNotExist if the path doesn't exist.
// Returns ErrIsDirectory if the path is a directory.
func (m *MemoryMount) Read(ctx context.Context, p string, offset int64, data []byte) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	file, exists := m.files[p]
	if !exists {
		return 0, vfs.ErrNotExist
	}

	if file.isDir {
		return 0, vfs.ErrIsDirectory
	}

	// If offset is beyond file size, return EOF
	if offset >= file.size {
		return 0, io.EOF
	}

	// Calculate how many bytes we can actually read
	available := file.size - offset
	toRead := int64(len(data))
	if toRead > available {
		toRead = available
	}

	// Copy data from file buffer
	n := copy(data, file.data[offset:offset+toRead])

	return n, nil
}

// Write writes data to the file at path starting at offset.
// If offset is beyond current file size, the gap is filled with zeros.
// Returns the number of bytes written and any error encountered.
// Returns ErrNotExist if the path doesn't exist.
// Returns ErrIsDirectory if the path is a directory.
func (m *MemoryMount) Write(ctx context.Context, p string, offset int64, data []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[p]
	if !exists {
		return 0, vfs.ErrNotExist
	}

	if file.isDir {
		return 0, vfs.ErrIsDirectory
	}

	// Calculate new size if writing beyond current end
	writeEnd := offset + int64(len(data))
	if writeEnd > file.size {
		// Need to expand the file
		newData := make([]byte, writeEnd)

		// Copy existing data
		copy(newData, file.data)

		// Fill gap with zeros if offset is beyond current size
		// (gap between file.size and offset is already zero from make())

		// Copy new data at offset
		copy(newData[offset:], data)

		file.data = newData
		file.size = writeEnd
	} else {
		// Writing within existing file bounds
		copy(file.data[offset:], data)
	}

	file.modTime = time.Now()

	return len(data), nil
}

// Delete removes the object at the given path.
// If force is true and the object is a directory, removes all children recursively.
// If force is false and the directory is not empty, returns an error.
// Returns ErrNotExist if the path doesn't exist.
func (m *MemoryMount) Delete(ctx context.Context, p string, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[p]
	if !exists {
		return vfs.ErrNotExist
	}

	// If it's a directory and force is false, check if it has children
	if file.isDir && !force {
		prefix := p
		if prefix != "" {
			prefix += "/"
		}

		for filePath := range m.files {
			if filePath != p && strings.HasPrefix(filePath, prefix) {
				return fmt.Errorf("directory not empty")
			}
		}
	}

	// Remove file or directory
	if force && file.isDir {
		// Remove all children
		prefix := p
		if prefix != "" {
			prefix += "/"
		}

		for filePath := range m.files {
			if filePath == p || strings.HasPrefix(filePath, prefix) {
				delete(m.files, filePath)
			}
		}
	} else {
		delete(m.files, p)
	}

	return nil
}

// List returns all objects under the given path.
// For files, returns a single entry. For directories, returns all direct children.
// Returns ErrNotExist if the path doesn't exist.
func (m *MemoryMount) List(ctx context.Context, p string) ([]*vfs.VirtualObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dir, exists := m.files[p]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	// For files, return single entry
	if !dir.isDir {
		mode := vfs.VirtualFileMode(0644)
		return []*vfs.VirtualObjectInfo{
			{
				Path:     p,
				Name:     path.Base(p),
				Type:     vfs.ObjectTypeFile,
				Size:     dir.size,
				Mode:     mode,
				ModTime:  dir.modTime,
				Metadata: copyMetadata(dir.metadata),
			},
		}, nil
	}

	// For directories, return children
	var objects []*vfs.VirtualObjectInfo
	prefix := p
	if prefix != "" {
		prefix += "/"
	}

	seen := make(map[string]bool)
	for filePath, file := range m.files {
		if strings.EqualFold(filePath, p) {
			continue
		}

		if !strings.HasPrefix(filePath, prefix) {
			continue
		}

		rel := strings.TrimPrefix(filePath, prefix)
		parts := strings.Split(rel, "/")
		if len(parts) == 0 {
			continue
		}

		childName := parts[0]
		if seen[childName] {
			continue
		}
		seen[childName] = true

		childPath := prefix + childName
		isChildDir := file.isDir
		if len(parts) > 1 {
			isChildDir = true
		}

		objType := vfs.ObjectTypeFile
		mode := vfs.VirtualFileMode(0644)
		size := file.size

		if isChildDir {
			objType = vfs.ObjectTypeDirectory
			mode = vfs.ModeDir | 0755
			size = 0
		}

		objects = append(objects, &vfs.VirtualObjectInfo{
			Path:     childPath,
			Name:     childName,
			Type:     objType,
			Size:     size,
			Mode:     mode,
			ModTime:  file.modTime,
			Metadata: copyMetadata(file.metadata),
		})
	}

	return objects, nil
}

// Stat returns information about the object at the given path.
// Returns ErrNotExist if the path doesn't exist.
func (m *MemoryMount) Stat(ctx context.Context, p string) (*vfs.VirtualObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	file, exists := m.files[p]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	objType := vfs.ObjectTypeFile
	if file.isDir {
		objType = vfs.ObjectTypeDirectory
	}

	mode := vfs.VirtualFileMode(0644)
	if file.isDir {
		mode = vfs.ModeDir | 0755
	}

	return &vfs.VirtualObjectInfo{
		Path:     p,
		Name:     path.Base(p),
		Type:     objType,
		Size:     file.size,
		Mode:     mode,
		ModTime:  file.modTime,
		Metadata: copyMetadata(file.metadata),
	}, nil
}

// Truncate changes the size of the file at path.
// If the file is larger than size, the extra data is discarded.
// If the file is smaller than size, it is extended with zero bytes.
// Returns ErrNotExist if the path doesn't exist.
// Returns ErrIsDirectory if the path is a directory.
func (m *MemoryMount) Truncate(ctx context.Context, p string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[p]
	if !exists {
		return vfs.ErrNotExist
	}

	if file.isDir {
		return vfs.ErrIsDirectory
	}

	if size == file.size {
		return nil // No change needed
	}

	if size < file.size {
		// Shrink file
		file.data = file.data[:size]
		file.size = size
	} else {
		// Expand file with zeros
		newData := make([]byte, size)
		copy(newData, file.data)
		file.data = newData
		file.size = size
	}

	file.modTime = time.Now()

	return nil
}

// copyMetadata creates a deep copy of a metadata map.
// Returns nil if the source is nil.
func copyMetadata(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
