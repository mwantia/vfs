package vfs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// VirtualFileSystem is the main VFS manager that handles mount points and delegates
// file operations to the appropriate mount handlers. It provides a Unix-like filesystem
// abstraction with support for nested mounts and thread-safe operations.
type VirtualFileSystem struct {
	mu      sync.RWMutex
	mounts  map[string]*VirtualMountEntry
	streams map[string]VirtualFile // Active streams keyed by absolute path
}

// VirtualMountEntry represents a single mount point with its handler and metadata.
type VirtualMountEntry struct {
	mount   VirtualMount         // The storage backend handler
	options *VirtualMountOptions // Metadata options about the mount
}

// NewVfs creates a new VirtualFileSystem instance with no initial mounts.
func NewVfs() *VirtualFileSystem {
	return &VirtualFileSystem{
		mounts:  make(map[string]*VirtualMountEntry),
		streams: make(map[string]VirtualFile),
	}
}

// Open opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (vfs *VirtualFileSystem) Open(ctx context.Context, path string, flags VirtualAccessMode) (VirtualFile, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}
	// Check if file is already open
	vfs.mu.RLock()
	if _, exists := vfs.streams[absPath]; exists {
		vfs.mu.RUnlock()
		return nil, ErrInUse
	}
	vfs.mu.RUnlock()

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	if entry.options.ReadOnly {
		if flags&AccessModeWrite != 0 || flags&AccessModeCreate != 0 || flags&AccessModeExcl != 0 {
			return nil, ErrReadOnly
		}
	}

	relPath := ToRelativePath(absPath, entry.options.Path)
	// Try to get file info
	info, err := entry.mount.Stat(ctx, relPath)
	if err != nil {
		if err == ErrNotExist {
			// Create file if it doesn't exist and CREATE flag is set
			if flags.HasCreate() {
				if err := entry.mount.Create(ctx, relPath, false); err != nil {
					return nil, err
				}
				// Refresh info after creation
				info, err = entry.mount.Stat(ctx, relPath)
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
		if err := entry.mount.Truncate(ctx, relPath, 0); err != nil {
			return nil, err
		}
	}

	stream := &virtualFileImpl{
		vfs:     vfs,
		mount:   entry.mount,
		path:    relPath,
		absPath: absPath,
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
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	relPath := ToRelativePath(absPath, entry.options.Path)
	buffer := make([]byte, size)
	n, err := entry.mount.Read(ctx, relPath, offset, buffer)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return buffer[:n], nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (vfs *VirtualFileSystem) Write(ctx context.Context, path string, offset int64, data []byte) (int64, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return 0, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return 0, err
	}

	if entry.options.ReadOnly {
		return 0, ErrReadOnly
	}

	relPath := ToRelativePath(absPath, entry.options.Path)
	n, err := entry.mount.Write(ctx, relPath, offset, data)
	return int64(n), err
}

// Close closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (vfs *VirtualFileSystem) Close(ctx context.Context, path string, force bool) error {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	vfs.mu.Lock()
	stream, exists := vfs.streams[absPath]
	vfs.mu.Unlock()

	// No stream open for this path
	if !exists {
		return nil
	}

	// Avoid closing busy streams (unless forced)
	if stream.IsBusy() && !force {
		return ErrBusy
	}

	return stream.Close()
}

// MkDir creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (vfs *VirtualFileSystem) MkDir(ctx context.Context, path string) error {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return err
	}

	if entry.options.ReadOnly {
		return ErrReadOnly
	}

	relPath := ToRelativePath(absPath, entry.options.Path)
	return entry.mount.Create(ctx, relPath, true)
}

// RmDir removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (vfs *VirtualFileSystem) RmDir(ctx context.Context, path string) error {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return err
	}

	if entry.options.ReadOnly {
		return ErrReadOnly
	}

	relPath := ToRelativePath(absPath, entry.options.Path)
	return entry.mount.Delete(ctx, relPath, false)
}

// Unlink removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (vfs *VirtualFileSystem) Unlink(ctx context.Context, path string) error {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return err
	}

	if entry.options.ReadOnly {
		return ErrReadOnly
	}

	relPath := ToRelativePath(absPath, entry.options.Path)

	// Check if it's a directory
	info, err := entry.mount.Stat(ctx, relPath)
	if err != nil {
		return err
	}

	if info.Type == ObjectTypeDirectory {
		return ErrIsDirectory
	}

	return entry.mount.Delete(ctx, relPath, false)
}

// Rename moves or renames a file or directory from oldPath to newPath.
// Returns an error if the operation cannot be completed.
func (vfs *VirtualFileSystem) Rename(ctx context.Context, oldPath string, newPath string) error {
	return fmt.Errorf("vfs: not implemented")
}

// ReadDir returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (vfs *VirtualFileSystem) ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	relPath := ToRelativePath(absPath, entry.options.Path)
	objects, err := entry.mount.List(ctx, relPath)
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
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	relPath := ToRelativePath(absPath, entry.options.Path)
	info, err := entry.mount.Stat(ctx, relPath)
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
	return fmt.Errorf("vfs: not implemented")
}

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (vfs *VirtualFileSystem) Mount(ctx context.Context, path string, mount VirtualMount, opts ...VirtualMountOption) error {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	options := &VirtualMountOptions{
		Path:      absPath,
		MountTime: time.Now(),
	}

	for _, opt := range opts {
		opt(options)
	}

	mountPath := options.Path
	if len(mountPath) == 0 {
		return ErrInvalidPath
	}

	if _, exists := vfs.mounts[mountPath]; exists {
		return ErrAlreadyMounted
	}

	// Check if parent mount denies nesting
	if parent, err := vfs.relativeMountForPath(mountPath); err == nil {
		if parent.options.DenyNesting {
			return ErrNestingDenied
		}
	}

	vfs.mounts[mountPath] = &VirtualMountEntry{
		mount:   mount,
		options: options,
	}

	return nil
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (vfs *VirtualFileSystem) Unmount(ctx context.Context, path string) error {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if _, exists := vfs.mounts[absPath]; !exists {
		return ErrNotMounted
	}

	if vfs.hasChildMounts(absPath) {
		return ErrMountBusy
	}

	delete(vfs.mounts, absPath)
	return nil
}

// Lookup checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (vfs *VirtualFileSystem) Lookup(ctx context.Context, path string) (bool, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return false, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return false, err
	}

	relPath := ToRelativePath(absPath, entry.options.Path)
	info, err := entry.mount.Stat(ctx, relPath)
	if err != nil {
		return false, err
	}

	return (info != nil), nil
}

// GetAttr retrieves extended attributes and metadata for the object at path.
// Returns detailed object information including metadata.
func (vfs *VirtualFileSystem) GetAttr(ctx context.Context, path string) (*VirtualObjectInfo, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	relPath := ToRelativePath(absPath, entry.options.Path)

	return entry.mount.Stat(ctx, relPath)
}

// SetAttr updates extended attributes and metadata for the object at path.
// Returns true if the attributes were updated, false if the path doesn't exist.
func (vfs *VirtualFileSystem) SetAttr(ctx context.Context, path string, info VirtualObjectInfo) (bool, error) {
	return false, fmt.Errorf("vfs: not implemented")
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

func (vfs *VirtualFileSystem) relativeMountForPath(path string) (*VirtualMountEntry, error) {
	// Skip, if no entries have been mounted yet
	if len(vfs.mounts) == 0 {
		return nil, ErrNotMounted
	}

	vfs.mu.RLock()

	var best *VirtualMountEntry
	for mp, entry := range vfs.mounts {
		if hasPrefix(path, mp) {
			if len(path) == len(mp) || path[len(mp)-1] == '/' {
				if best == nil || len(mp) > len(best.options.Path) {
					best = entry
				}
			}
		}
	}

	vfs.mu.RUnlock()

	if best == nil {
		return nil, ErrNotMounted
	}

	return best, nil
}

func (vfs *VirtualFileSystem) hasChildMounts(parent string) bool {
	for mount := range vfs.mounts {
		if mount != parent && hasPrefix(mount, parent) {
			return true
		}
	}

	return false
}
