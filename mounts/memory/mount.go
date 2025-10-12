package memory

import (
	"context"
	"io"
	"path"
	"strings"
	"time"

	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/data"
)

// Mount is called when the mount is being attached to the VFS.
// Initializes the root directory in all three layers.
func (m *MetaMemoryMount) Mount(ctx context.Context, path string, vfs *vfs.VirtualFileSystem) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create root inode
	n := time.Now()
	ino_t := m.generateInodeID()
	rootInode := &data.VirtualInode{
		ID:   ino_t,
		Name: "",
		Type: data.NodeTypeMount,
		Mode: data.ModeDir,

		AccessTime: n,
		ModifyTime: n,
		ChangeTime: n,
		CreateTime: n,
	}

	m.paths.Set("", ino_t)
	m.inodes[ino_t] = rootInode

	return nil
}

// Unmount is called when the mount is being detached from the VFS.
// Clears all three layers to prevent memory leaks.
func (m *MetaMemoryMount) Unmount(ctx context.Context, path string, vfs *vfs.VirtualFileSystem) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.paths.Clear()
	for k := range m.inodes {
		delete(m.inodes, k)
	}
	for k := range m.datas {
		delete(m.datas, k)
	}

	return nil
}

// Create creates a new file or directory at the given path.
// Returns ErrExist if the path already exists.
// Parent directories must exist - they are NOT created automatically.
func (m *MetaMemoryMount) Create(ctx context.Context, p string, isDir bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if path already exists in B-tree
	if _, exists := m.paths.Get(p); exists {
		return vfs.ErrExist
	}

	// Verify parent directory exists
	parent := path.Dir(p)
	if parent != "." && parent != "" {
		parentID, exists := m.paths.Get(parent)
		if !exists {
			return vfs.ErrNotExist
		}

		parentInode, exists := m.inodes[parentID]
		if !exists {
			return vfs.ErrNotExist
		}

		if !parentInode.IsDir() {
			return vfs.ErrNotDirectory
		}
	}

	ino_t := m.generateInodeID()
	name := path.Base(p)

	// Create inode
	var newInode *data.VirtualInode
	if isDir {
		newInode = data.NewDirectoryInode(ino_t, data.ModeDir|0755)
	} else {
		newInode = data.NewFileInode(ino_t, 0, 0644)
	}
	newInode.Name = name

	m.paths.Set(p, ino_t)
	m.inodes[ino_t] = newInode

	return nil
}

// Read reads up to len(data) bytes from the file at path starting at offset.
// Returns the number of bytes read and any error encountered.
// Returns ErrNotExist if the path doesn't exist.
// Returns ErrIsDirectory if the path is a directory.
func (m *MetaMemoryMount) Read(ctx context.Context, p string, offset int64, data []byte) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inode, err := m.GetInode(ctx, p)
	if err != nil {
		return 0, err
	}

	if inode.IsDir() {
		return 0, vfs.ErrIsDirectory
	}

	if offset >= inode.Size {
		return 0, io.EOF
	}

	buffer, exists := m.datas[inode.ID]
	if !exists {
		return 0, nil
	}

	// Calculate how many bytes we can actually read
	available := inode.Size - offset
	toRead := min(int64(len(data)), available)
	// Copy data from file buffer
	n := copy(data, buffer[offset:offset+toRead])
	return n, nil
}

// Write writes data to the file at path starting at offset.
// If offset is beyond current file size, the gap is filled with zeros.
// Returns the number of bytes written and any error encountered.
// Returns ErrNotExist if the path doesn't exist.
// Returns ErrIsDirectory if the path is a directory.
func (m *MetaMemoryMount) Write(ctx context.Context, p string, offset int64, data []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	inode, err := m.GetInode(ctx, p)
	if err != nil {
		return 0, err
	}

	if inode.IsDir() {
		return 0, vfs.ErrIsDirectory
	}

	writeEnd := offset + int64(len(data))

	// Get existing buffer or create new one
	buffer, exists := m.datas[inode.ID]
	if !exists {
		buffer = make([]byte, 0)
	}

	// Determine the new required size
	newSize := max(writeEnd, inode.Size)

	// Expand buffer if needed
	if int64(len(buffer)) < newSize {
		newBuffer := make([]byte, newSize)
		copy(newBuffer, buffer)
		buffer = newBuffer
	}

	// Write the data
	copy(buffer[offset:], data)

	// Update storage
	m.datas[inode.ID] = buffer
	if writeEnd > inode.Size {
		inode.Size = writeEnd
	}

	inode.ModifyTime = time.Now()
	return len(data), nil
}

// Delete removes the object at the given path.
// If force is true and the object is a directory, removes all children recursively.
// Returns ErrNotExist if the path doesn't exist.
func (m *MetaMemoryMount) Delete(ctx context.Context, p string, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inode, err := m.GetInode(ctx, p)
	if err != nil {
		return err
	}

	if inode.IsDir() {
		// Directories can only be deleted with force=true
		if !force {
			return vfs.ErrIsDirectory
		}

		// Build prefix for children lookup
		prefix := p
		if prefix != "" {
			prefix += "/"
		}

		// Collect all paths to delete (including this directory)
		var pathsToDelete []string
		pathsToDelete = append(pathsToDelete, p)

		// Use B-tree range scan to find all children
		m.paths.Scan(func(childPath string, childID string) bool {
			if strings.HasPrefix(childPath, prefix) {
				pathsToDelete = append(pathsToDelete, childPath)
			}
			return true // Continue scanning
		})

		// Delete all collected paths
		for _, delPath := range pathsToDelete {
			delID, exists := m.paths.Get(delPath)
			if !exists {
				continue
			}

			m.paths.Delete(delPath)

			// Check if any other paths reference this inode (hard links)
			hasOtherLinks := false
			m.paths.Scan(func(key string, value string) bool {
				if value == delID {
					hasOtherLinks = true
					return false // Stop scanning
				}
				return true
			})

			// Only delete inode and data if no other links exist
			if !hasOtherLinks {
				delete(m.datas, delID)
				delete(m.inodes, delID)
			}
		}

	} else {
		// Delete file
		m.paths.Delete(p)

		// Check for hard links
		hasOtherLinks := false
		m.paths.Scan(func(key string, value string) bool {
			if value == inode.ID {
				hasOtherLinks = true
				return false
			}
			return true
		})

		// Only delete inode and data if no other links exist
		if !hasOtherLinks {
			delete(m.datas, inode.ID)
			delete(m.inodes, inode.ID)
		}
	}

	return nil
}

// List returns all objects under the given path.
// For files, returns a single entry. For directories, returns all direct children.
// Returns ErrNotExist if the path doesn't exist.
func (m *MetaMemoryMount) List(ctx context.Context, p string) ([]*data.VirtualFileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inode, err := m.GetInode(ctx, p)
	if err != nil {
		return nil, err
	}

	// For files, return single entry
	if !inode.IsDir() {
		return []*data.VirtualFileInfo{
			inode.ToFileInfo(p),
		}, nil
	}

	// For directories, use B-tree range scan to find children
	prefix := p
	if prefix != "" {
		prefix += "/"
	}
	prefixLen := len(prefix)

	// Use map to deduplicate direct children
	children := make(map[string]*data.VirtualFileInfo)

	// B-tree range scan: iterate over all paths starting with prefix
	m.paths.Scan(func(childPath string, childID string) bool {
		// Check if this path is under our directory
		if !strings.HasPrefix(childPath, prefix) {
			return true // Continue scanning (paths are ordered)
		}

		// Get relative path
		rel := childPath[prefixLen:]

		// Check if this is a direct child (no slash in relative path)
		if slashIdx := strings.IndexByte(rel, '/'); slashIdx > 0 {
			// This is a nested child - only track the first segment
			childName := rel[:slashIdx]
			if _, seen := children[childName]; !seen {
				// Look up the directory inode
				dirPath := prefix + childName
				if dirID, exists := m.paths.Get(dirPath); exists {
					if dirInode, exists := m.inodes[dirID]; exists {
						children[childName] = dirInode.ToFileInfo(dirPath)
					}
				}
			}
		} else if rel != "" {
			// Direct child - add it
			if childInode, exists := m.inodes[childID]; exists {
				children[rel] = childInode.ToFileInfo(childPath)
			}
		}

		return true // Continue scanning
	})

	// Convert map to slice
	result := make([]*data.VirtualFileInfo, 0, len(children))
	for _, info := range children {
		result = append(result, info)
	}

	return result, nil
}

// Stat returns information about the object at the given path.
// Returns ErrNotExist if the path doesn't exist.
func (m *MetaMemoryMount) Stat(ctx context.Context, p string) (*data.VirtualFileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inode, err := m.GetInode(ctx, p)
	if err != nil {
		return nil, err
	}

	return inode.ToFileInfo(p), nil
}

// Truncate changes the size of the file at path.
// If the file is larger than size, the extra data is discarded.
// If the file is smaller than size, it is extended with zero bytes.
// Returns ErrNotExist if the path doesn't exist.
// Returns ErrIsDirectory if the path is a directory.
func (m *MetaMemoryMount) Truncate(ctx context.Context, p string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inode, err := m.GetInode(ctx, p)
	if err != nil {
		return err
	}

	if inode.IsDir() {
		return vfs.ErrIsDirectory
	}

	if size == inode.Size {
		return nil // No changes needed
	}

	buffer, exists := m.datas[inode.ID]
	if exists {
		if size < inode.Size {
			// Shrink file
			m.datas[inode.ID] = buffer[:size]
		} else {
			// Expand file with zeros
			newData := make([]byte, size)
			copy(newData, buffer)
			m.datas[inode.ID] = newData
		}
	}

	inode.Size = size
	inode.ModifyTime = time.Now()

	return nil
}
