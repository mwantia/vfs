package vfs

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
)

// VirtualFileSystem is the main VFS manager that handles mount points and delegates
// file operations to the appropriate mount handlers. It provides a Unix-like filesystem
// abstraction with support for nested mounts and thread-safe operations.
type VirtualFileSystem struct {
	mu      sync.RWMutex
	mnts    map[string]*VirtualMountEntry
	streams map[string]*VirtualFileStream
}

// VirtualMountEntry represents a single mount point with its handler and metadata.
type VirtualMountEntry struct {
	path    string
	mount   *mount.VirtualMount        // The storage backend handler
	options *mount.VirtualMountOptions // Metadata options about the mount
}

// NewVfs creates a new VirtualFileSystem instance with no initial mounts.
func NewVfs() *VirtualFileSystem {
	return &VirtualFileSystem{
		mnts:    make(map[string]*VirtualMountEntry),
		streams: make(map[string]*VirtualFileStream),
	}
}

// Open opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (vfs *VirtualFileSystem) Open(ctx context.Context, path string, flags data.VirtualAccessMode) (mount.VirtualFile, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}
	// Check if file is already open
	vfs.mu.RLock()
	if _, exists := vfs.streams[absPath]; exists {
		vfs.mu.RUnlock()
		return nil, data.ErrInUse
	}
	vfs.mu.RUnlock()

	mnt, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	if mnt.options.ReadOnly {
		if flags&data.AccessModeWrite != 0 || flags&data.AccessModeCreate != 0 || flags&data.AccessModeExcl != 0 {
			return nil, data.ErrReadOnly
		}
	}

	relPath := ToRelativePath(absPath, mnt.path)
	// Try to get file info
	info, err := mnt.mount.Stat(ctx, relPath)
	if err != nil {
		if err == data.ErrNotExist {
			// Create file if it doesn't exist and CREATE flag is set
			if flags.HasCreate() {
				// TODO :: Find correct filetype and filemode
				if err := mnt.mount.Create(ctx, relPath, data.FileTypeFile, 0x777); err != nil {
					return nil, err
				}
				// Refresh info after creation
				info, err = mnt.mount.Stat(ctx, relPath)
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
			return nil, data.ErrExist
		}

		if info.IsDir() {
			return nil, data.ErrIsDirectory
		}
	}

	// Determine initial offset
	offset := int64(0)
	if flags.HasAppend() {
		// For append mode, start at end of file
		offset = info.Size
	} else if flags.HasTrunc() && (flags.IsWriteOnly() || flags.IsReadWrite()) {
		// Only truncate if TRUNC flag is set and we have write access
		if err := mnt.mount.Truncate(ctx, relPath, 0); err != nil {
			return nil, err
		}
	}

	stream := NewVirtualFileStream(ctx, vfs, mnt, relPath, offset, flags)
	// Register stream with absolute path as key
	vfs.mu.Lock()
	vfs.streams[absPath] = stream
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

	relPath := ToRelativePath(absPath, entry.path)
	dat := make([]byte, size)
	n, err := entry.mount.Read(ctx, relPath, offset, dat)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return dat[:n], nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (vfs *VirtualFileSystem) Write(ctx context.Context, path string, offset int64, dat []byte) (int64, error) {
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
		return 0, data.ErrReadOnly
	}

	relPath := ToRelativePath(absPath, entry.path)
	n, err := entry.mount.Write(ctx, relPath, offset, dat)
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
		return data.ErrBusy
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
		return data.ErrReadOnly
	}

	relPath := ToRelativePath(absPath, entry.path)
	// Find correct filetype for directory (or universal always the same?)
	return entry.mount.Create(ctx, relPath, data.FileTypeDirectory, 0x777)
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
		return data.ErrReadOnly
	}

	relPath := ToRelativePath(absPath, entry.path)
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
		return data.ErrReadOnly
	}

	relPath := ToRelativePath(absPath, entry.path)

	// Check if it's a directory
	info, err := entry.mount.Stat(ctx, relPath)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return data.ErrIsDirectory
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
func (vfs *VirtualFileSystem) ReadDir(ctx context.Context, path string) ([]*data.VirtualFileMetadata, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	relPath := ToRelativePath(absPath, entry.path)
	objects, err := entry.mount.List(ctx, relPath)
	if err != nil {
		return nil, err
	}

	return objects, nil
}

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (vfs *VirtualFileSystem) Stat(ctx context.Context, path string) (*data.VirtualFileMetadata, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	relPath := ToRelativePath(absPath, entry.path)
	info, err := entry.mount.Stat(ctx, relPath)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// Chmod changes the mode (permissions) of the file at path.
// Returns an error if the operation is not supported or fails.
func (vfs *VirtualFileSystem) Chmod(ctx context.Context, path string, mode data.VirtualFileMode) error {
	return fmt.Errorf("vfs: not implemented")
}

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (vfs *VirtualFileSystem) Mount(ctx context.Context, path string, mnt *mount.VirtualMount, opts ...mount.VirtualMountOption) error {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	if len(absPath) == 0 {
		return data.ErrInvalidPath
	}

	options := mount.NewDefaultOptions()

	for _, opt := range opts {
		opt(options)
	}

	// Check if parent mount denies nesting BEFORE acquiring write lock
	if parent, err := vfs.relativeMountForPath(absPath); err == nil {
		if !parent.options.Nesting {
			return data.ErrNestingDenied
		}
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if _, exists := vfs.mnts[absPath]; exists {
		return data.ErrAlreadyMounted
	}

	for existingPath, entry := range vfs.mnts {
		if entry.mount == mnt {
			if hasPrefix(absPath, existingPath) && absPath != existingPath {
				return data.ErrCircularReference
			}

			if hasPrefix(existingPath, absPath) && absPath != existingPath {
				return data.ErrCircularReference
			}
		}
	}

	if err := mnt.Mount(ctx); err != nil {
		return data.ErrMountFailed
	}

	vfs.mnts[absPath] = &VirtualMountEntry{
		mount:   mnt,
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

	entry, exists := vfs.mnts[absPath]
	if !exists {
		return data.ErrNotMounted
	}

	if vfs.hasChildMounts(absPath) {
		return data.ErrMountBusy
	}

	if err := entry.mount.Unmount(ctx); err != nil {
		return data.ErrUnmountFailed
	}

	delete(vfs.mnts, absPath)
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

	relPath := ToRelativePath(absPath, entry.path)
	info, err := entry.mount.Stat(ctx, relPath)
	if err != nil {
		return false, err
	}

	return (info != nil), nil
}

// GetAttr retrieves extended attributes and metadata for the object at path.
// Returns detailed object information including metadata.
func (vfs *VirtualFileSystem) GetAttr(ctx context.Context, path string) (*data.VirtualFileMetadata, error) {
	// Always start with an absolute path
	absPath, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	entry, err := vfs.relativeMountForPath(absPath)
	if err != nil {
		return nil, err
	}

	relPath := ToRelativePath(absPath, entry.path)

	return entry.mount.Stat(ctx, relPath)
}

// SetAttr updates extended attributes and metadata for the object at path.
// Returns true if the attributes were updated, false if the path doesn't exist.
func (vfs *VirtualFileSystem) SetAttr(ctx context.Context, path string, info data.VirtualFileMetadata) (bool, error) {
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
	if len(vfs.mnts) == 0 {
		return nil, data.ErrNotMounted
	}

	vfs.mu.RLock()

	var best *VirtualMountEntry
	for mp, entry := range vfs.mnts {
		if hasPrefix(path, mp) {
			// For root mount ("/"), it matches everything
			// For other mounts, ensure exact match or path continues with /
			if mp == "/" || len(path) == len(mp) || (len(path) > len(mp) && path[len(mp)] == '/') {
				if best == nil || len(mp) > len(best.path) {
					best = entry
				}
			}
		}
	}

	vfs.mu.RUnlock()

	if best == nil {
		return nil, data.ErrNotMounted
	}

	return best, nil
}

func (vfs *VirtualFileSystem) hasChildMounts(parent string) bool {
	for mount := range vfs.mnts {
		if mount != parent && hasPrefix(mount, parent) {
			return true
		}
	}

	return false
}
