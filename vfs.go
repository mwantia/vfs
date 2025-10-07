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

// OpenRead opens a file for reading and returns a reader.
// The returned VirtualReader must be closed by the caller.
func (vfs *VirtualFileSystem) OpenRead(ctx context.Context, path string, flags VirtualAccessMode) (VirtualReader, error) {
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

	info, err := mount.Stat(ctx, relPath)
	if err != nil {
		return nil, err
	}

	if info.Type == ObjectTypeDirectory {
		return nil, ErrIsDirectory
	}

	stream := &virtualReaderImpl{
		vfs:     vfs,
		mount:   mount,
		path:    relPath, // Relative path for mount operations
		absPath: path,    // Absolute path for stream tracking
		offset:  0,
		ctx:     ctx,
		closed:  false,
	}

	// Register stream with absolute path as key
	vfs.mu.Lock()
	vfs.streams[path] = stream
	vfs.mu.Unlock()

	return stream, nil
}

// OpenWrite opens a file for writing and returns a reader/writer.
// The returned VirtualReadWriter must be closed by the caller.
func (vfs *VirtualFileSystem) OpenWrite(ctx context.Context, path string, flags VirtualAccessMode) (VirtualReadWriter, error) {
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
		// For append mode, get current file size
		offset = info.Size
	} else if flags.HasTrunc() {
		// Only truncate if TRUNC flag is explicitly set
		if err := mount.Truncate(ctx, relPath, 0); err != nil {
			return nil, err
		}
	}

	stream := &virtualWriterImpl{
		vfs:     vfs,
		mount:   mount,
		path:    relPath, // Relative path for mount operations
		absPath: path,    // Absolute path for stream tracking
		offset:  offset,
		ctx:     ctx,
		closed:  false,
	}

	// Register stream with absolute path as key
	vfs.mu.Lock()
	vfs.streams[path] = stream
	vfs.mu.Unlock()

	return stream, nil
}

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

func (vfs *VirtualFileSystem) Write(ctx context.Context, path string, offset int64, data []byte) (int64, error) {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return 0, err
	}

	n, err := mount.Write(ctx, relPath, offset, data)
	return int64(n), err
}

func (vfs *VirtualFileSystem) Close(ctx context.Context, path string) error {
	vfs.mu.Lock()
	stream, exists := vfs.streams[path]
	vfs.mu.Unlock()

	if !exists {
		return nil // No stream open for this path
	}

	return stream.Close()
}

func (vfs *VirtualFileSystem) MkDir(ctx context.Context, path string) error {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return err
	}

	return mount.Create(ctx, relPath, true)
}

func (vfs *VirtualFileSystem) RmDir(ctx context.Context, path string) error {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return err
	}

	return mount.Delete(ctx, relPath, false)
}

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

func (vfs *VirtualFileSystem) Rename(ctx context.Context, oldPath string, newPath string) error {
	_, _, err := vfs.resolveMountAndPath(oldPath)
	if err != nil {
		return err
	}

	return nil
}

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

func (vfs *VirtualFileSystem) Chmod(ctx context.Context, path string, mode VirtualFileMode) error {
	_, _, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return err
	}

	return nil
}

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

func (vfs *VirtualFileSystem) GetAttr(ctx context.Context, path string) (*VirtualObjectInfo, error) {
	mount, relPath, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return nil, err
	}

	return mount.Stat(ctx, relPath)
}

func (vfs *VirtualFileSystem) SetAttr(ctx context.Context, path string, info VirtualObjectInfo) (bool, error) {
	_, _, err := vfs.resolveMountAndPath(path)
	if err != nil {
		return false, err
	}

	return false, nil
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
