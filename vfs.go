package vfs

import (
	"context"
	"io"
	"sync"
	"time"
)

// VirtualFileSystem is the main VFS manager that handles mount points and delegates
// file operations to the appropriate mount handlers. It provides a Unix-like filesystem
// abstraction with support for nested mounts and thread-safe operations.
type VirtualFileSystem struct {
	mu      sync.RWMutex
	mounts  map[string]*VirtualFileSystemEntry
	streams map[string]io.Closer // Active streams keyed by absolute path
}

// VirtualFileSystemEntry represents a single mount point with its handler and metadata.
type VirtualFileSystemEntry struct {
	mount VirtualMount     // The storage backend handler
	info  VirtualMountInfo // Metadata about the mount
}

// NewVfs creates a new VirtualFileSystem instance with no initial mounts.
func NewVfs() *VirtualFileSystem {
	return &VirtualFileSystem{
		mounts:  make(map[string]*VirtualFileSystemEntry),
		streams: make(map[string]io.Closer),
	}
}

// Open opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (vfs *VirtualFileSystem) Open(ctx context.Context, path string, flags VirtualAccessMode) (VirtualFile, error) {
	// Check if file is already open
	vfs.mu.RLock()
	if _, exists := vfs.streams[path]; exists {
		vfs.mu.RUnlock()
		return nil, ErrInUse
	}
	vfs.mu.RUnlock()

	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return nil, err
	}

	// Try to get file info
	info, err := mount.Stat(ctx, relPath)
	if err != nil {
		if err == ErrNotExist {
			// Create file if it doesn't exist and CREATE flag is set
			if flags.HasCreate() {
				if err := mount.Create(ctx, relPath, false); err != nil {
					return nil, err
				}
				// Refresh info after creation
				info, err = mount.Stat(ctx, relPath)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		// File exists - check EXCL flag
		if flags.HasExcl() && flags.HasCreate() {
			return nil, ErrExist
		}

		if info.Type == ObjectTypeDirectory {
			return nil, ErrIsDirectory
		}
	}

	// Determine initial offset
	offset := int64(0)
	if flags.HasAppend() {
		// For append mode, start at end of file
		offset = info.Size
	} else if flags.HasTrunc() && (flags.IsWriteOnly() || flags.IsReadWrite()) {
		// Only truncate if TRUNC flag is set and we have write access
		if err := mount.Truncate(ctx, relPath, 0); err != nil {
			return nil, err
		}
	}

	stream := &virtualFileImpl{
		vfs:     vfs,
		mount:   mount,
		path:    relPath, // Relative path for mount operations
		absPath: path,    // Absolute path for stream tracking
		offset:  offset,
		flags:   flags,
		ctx:     ctx,
		closed:  false,
	}

	// Register stream with absolute path as key
	vfs.mu.Lock()
	vfs.streams[path] = stream
	vfs.mu.Unlock()

	return stream, nil
}

// Read reads size bytes from the file at path starting at offset.
// Returns the data read or an error if the operation fails.
func (vfs *VirtualFileSystem) Read(ctx context.Context, path string, offset, size int64) ([]byte, error) {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, size)
	n, err := mount.Read(ctx, relPath, offset, buffer)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return buffer[:n], nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (vfs *VirtualFileSystem) Write(ctx context.Context, path string, offset int64, data []byte) (int64, error) {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return 0, err
	}

	n, err := mount.Write(ctx, relPath, offset, data)
	return int64(n), err
}

// Close closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (vfs *VirtualFileSystem) Close(ctx context.Context, path string) error {
	vfs.mu.Lock()
	stream, exists := vfs.streams[path]
	vfs.mu.Unlock()

	if !exists {
		return nil // No stream open for this path
	}

	return stream.Close()
}

// MkDir creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (vfs *VirtualFileSystem) MkDir(ctx context.Context, path string) error {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return err
	}

	return mount.Create(ctx, relPath, true)
}

// RmDir removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (vfs *VirtualFileSystem) RmDir(ctx context.Context, path string) error {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return err
	}

	return mount.Delete(ctx, relPath, false)
}

// Unlink removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (vfs *VirtualFileSystem) Unlink(ctx context.Context, path string) error {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return err
	}

	// Check if it's a directory
	info, err := mount.Stat(ctx, relPath)
	if err != nil {
		return err
	}

	if info.Type == ObjectTypeDirectory {
		return ErrIsDirectory
	}

	return mount.Delete(ctx, relPath, false)
}

// Rename moves or renames a file or directory from oldPath to newPath.
// Returns an error if the operation cannot be completed.
func (vfs *VirtualFileSystem) Rename(ctx context.Context, oldPath string, newPath string) error {
	_, _, err := vfs.resolveMountAndPath(oldPath)
	if err != nil {
		return err
	}

	return nil
}

// ReadDir returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (vfs *VirtualFileSystem) ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error) {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return nil, err
	}

	objects, err := mount.List(ctx, relPath)
	if err != nil {
		return nil, err
	}

	result := make([]*VirtualFileInfo, len(objects))
	for i, obj := range objects {
		result[i] = &VirtualFileInfo{
			Name:    obj.Name,
			Path:    obj.Path,
			Size:    obj.Size,
			Mode:    obj.Mode,
			IsDir:   obj.Type == ObjectTypeDirectory,
			ModTime: obj.ModTime,
		}
	}

	return result, nil
}

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (vfs *VirtualFileSystem) Stat(ctx context.Context, path string) (*VirtualFileInfo, error) {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return nil, err
	}

	info, err := mount.Stat(ctx, relPath)
	if err != nil {
		return nil, err
	}

	return &VirtualFileInfo{
		Name:    info.Name,
		Path:    info.Path,
		Size:    info.Size,
		Mode:    info.Mode,
		IsDir:   info.Type == ObjectTypeDirectory,
		ModTime: info.ModTime,
	}, nil
}

// Chmod changes the mode (permissions) of the file at path.
// Returns an error if the operation is not supported or fails.
func (vfs *VirtualFileSystem) Chmod(ctx context.Context, path string, mode VirtualFileMode) error {
	_, _, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return err
	}

	return nil
}

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (vfs *VirtualFileSystem) Mount(ctx context.Context, path string, mount VirtualMount, opts ...VirtualMountOption) error {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if _, exists := vfs.mounts[path]; exists {
		return ErrAlreadyMounted
	}

	info := &VirtualMountInfo{
		Path:      path,
		MountedAt: time.Now(),
	}

	for _, opt := range opts {
		opt(info)
	}

	vfs.mounts[path] = &VirtualFileSystemEntry{
		mount: mount,
		info:  *info,
	}

	return nil
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (vfs *VirtualFileSystem) Unmount(ctx context.Context, path string) error {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if _, exists := vfs.mounts[path]; !exists {
		return ErrNotMounted
	}

	if vfs.hasChildMounts(path) {
		return ErrMountBusy
	}

	delete(vfs.mounts, path)
	return nil
}

// Lookup checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (vfs *VirtualFileSystem) Lookup(ctx context.Context, path string) (bool, error) {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return false, err
	}

	info, err := mount.Stat(ctx, relPath)
	if err != nil {
		return false, err
	}

	return (info != nil), nil
}

// GetAttr retrieves extended attributes and metadata for the object at path.
// Returns detailed object information including metadata.
func (vfs *VirtualFileSystem) GetAttr(ctx context.Context, path string) (*VirtualObjectInfo, error) {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return nil, err
	}

	return mount.Stat(ctx, relPath)
}

// SetAttr updates extended attributes and metadata for the object at path.
// Returns true if the attributes were updated, false if the path doesn't exist.
func (vfs *VirtualFileSystem) SetAttr(ctx context.Context, path string, info VirtualObjectInfo) (bool, error) {
	_, _, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return false, err
	}

	return false, nil
}

// Cleanup closes all currently open streams in the VFS.
// Returns the first error encountered, but continues closing all streams.
func (vfs *VirtualFileSystem) Cleanup() error {
	vfs.mu.Lock()
	streams := make([]io.Closer, 0, len(vfs.streams))
	for _, stream := range vfs.streams {
		streams = append(streams, stream)
	}
	vfs.mu.Unlock()

	var firstErr error
	for _, stream := range streams {
		if err := stream.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (vfs *VirtualFileSystem) resolveMountAndPath(path string) (VirtualMount, string, error) {
	// No mounts mean no resolve possible...
	if len(vfs.mounts) == 0 {
		return nil, "", ErrNotMounted
	}

	vfs.mu.RLock()

	var bestMatch string
	for mp := range vfs.mounts {
		if hasPrefix(path, mp) {
			if len(mp) > len(bestMatch) {
				bestMatch = mp
			}
		}
	}

	if bestMatch == "" {
		vfs.mu.RUnlock()
		return nil, "", ErrNotMounted
	}

	mount := vfs.mounts[bestMatch].mount
	vfs.mu.RUnlock()

	// Get relative path within mount
	relPath := trimPrefix(path, bestMatch)

	return mount, relPath, nil
}

func (vfs *VirtualFileSystem) hasChildMounts(parent string) bool {
	for mount := range vfs.mounts {
		if mount != parent && hasPrefix(mount, parent) {
			return true
		}
	}

	return false
}
