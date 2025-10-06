package vfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

type VirtualFileSystem struct {
	mu     sync.RWMutex
	mounts map[string]*VirtualFileSystemEntry
}

type VirtualFileSystemEntry struct {
	mount VirtualMount
	info  VirtualMountInfo
}

func NewVfs() *VirtualFileSystem {
	return &VirtualFileSystem{
		mounts: make(map[string]*VirtualFileSystemEntry),
	}
}

func (v *VirtualFileSystem) Mount(path string, mount VirtualMount, opts ...VirtualMountOption) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Normalize path
	path = cleanPath(path)

	// Check if already mounted
	if _, exists := v.mounts[path]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyMounted, path)
	}

	// Create mount info
	info := VirtualMountInfo{
		Path:      "/" + path,
		MountedAt: time.Now(),
	}

	// Apply options
	for _, opt := range opts {
		opt(&info)
	}

	// Store mount entry
	v.mounts[path] = &VirtualFileSystemEntry{
		mount: mount,
		info:  info,
	}

	return nil
}

// Unmount removes a filesystem handler from the given path.
// Returns ErrNotMounted if the path is not mounted.
// Returns ErrMountBusy if child mounts exist.
func (v *VirtualFileSystem) Unmount(path string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	path = cleanPath(path)

	// Check if mounted
	if _, exists := v.mounts[path]; !exists {
		return fmt.Errorf("%w: %s", ErrNotMounted, path)
	}

	// Check for child mounts
	if v.hasChildMounts(path) {
		return fmt.Errorf("%w: %s has child mounts", ErrMountBusy, path)
	}

	delete(v.mounts, path)
	return nil
}

// Mounts returns information about all mounted filesystems.
func (v *VirtualFileSystem) Mounts() []VirtualMountInfo {
	v.mu.RLock()
	defer v.mu.RUnlock()

	infos := make([]VirtualMountInfo, 0, len(v.mounts))
	for _, entry := range v.mounts {
		infos = append(infos, entry.info)
	}

	return infos
}

func (v *VirtualFileSystem) resolve(path string) (VirtualMount, string, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	path = cleanPath(path)

	// Find longest matching mount point
	var bestMatch string
	for mountPoint := range v.mounts {
		if hasPrefix(path, mountPoint) {
			if len(mountPoint) > len(bestMatch) {
				bestMatch = mountPoint
			}
		}
	}

	if bestMatch == "" && len(v.mounts) == 0 {
		return nil, "", fmt.Errorf("%w: no mounts configured", ErrNotMounted)
	}

	if bestMatch == "" {
		root, exists := v.mounts[""]
		if !exists {
			return nil, "", fmt.Errorf("%w: no mount for path /%s", ErrNotMounted, path)
		}

		rel := trimPrefix(path, bestMatch)
		return root.mount, rel, nil
	}

	// Calculate relative path
	relativePath := trimPrefix(path, bestMatch)

	return v.mounts[bestMatch].mount, relativePath, nil
}

// hasChildMounts checks if any mounts exist under the given parent path.
// Must be called with lock held.
func (v *VirtualFileSystem) hasChildMounts(parent string) bool {
	for mountPoint := range v.mounts {
		if mountPoint != parent && hasPrefix(mountPoint, parent) {
			return true
		}
	}
	return false
}

// Stat returns file information for the given path.
func (v *VirtualFileSystem) Stat(ctx context.Context, path string) (*VirtualFileInfo, error) {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return nil, err
	}

	objInfo, err := mount.Stat(ctx, relPath)
	if err != nil {
		return nil, err
	}

	return objectInfoToFileInfo(objInfo), nil
}

// ReadDir lists directory contents for the given path.
func (v *VirtualFileSystem) ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error) {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return nil, err
	}

	objInfos, err := mount.List(ctx, relPath)
	if err != nil {
		return nil, err
	}

	// Check if it's a directory
	if len(objInfos) == 1 && objInfos[0].Type != ObjectTypeDirectory {
		return nil, ErrNotDirectory
	}

	// Convert to FileInfo
	fileInfos := make([]*VirtualFileInfo, len(objInfos))
	for i, objInfo := range objInfos {
		fileInfos[i] = objectInfoToFileInfo(objInfo)
	}

	return fileInfos, nil
}

// Open opens a file for reading.
func (v *VirtualFileSystem) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return nil, err
	}

	obj, err := mount.Get(ctx, relPath)
	if err != nil {
		return nil, err
	}

	if obj.Info.Type == ObjectTypeDirectory {
		return nil, ErrIsDirectory
	}

	if obj.Data == nil {
		return io.NopCloser(bytes.NewReader([]byte{})), nil
	}

	// Wrap reader in NopCloser if it's not already a ReadCloser
	if rc, ok := obj.Data.(io.ReadCloser); ok {
		return rc, nil
	}

	return io.NopCloser(obj.Data), nil
}

// Create creates a new file for writing.
func (v *VirtualFileSystem) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return nil, err
	}

	// Return a writer that will create the file when closed
	return &fileWriter{
		mount: mount,
		path:  relPath,
		ctx:   ctx,
		buf:   new(bytes.Buffer),
	}, nil
}

// Remove deletes a file.
func (v *VirtualFileSystem) Remove(ctx context.Context, path string) error {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return err
	}

	// Check if it's a file
	objInfo, err := mount.Stat(ctx, relPath)
	if err != nil {
		return err
	}

	if objInfo.Type == ObjectTypeDirectory {
		return ErrIsDirectory
	}

	deleted, err := mount.Delete(ctx, relPath, false)
	if err != nil {
		return err
	}

	if !deleted {
		return ErrNotExist
	}

	return nil
}

// Mkdir creates a directory.
func (v *VirtualFileSystem) Mkdir(ctx context.Context, path string) error {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return err
	}

	obj := &VirtualObject{
		Info: VirtualObjectInfo{
			Path:    relPath,
			Type:    ObjectTypeDirectory,
			Mode:    ModeDir | 0755,
			ModTime: time.Now(),
		},
		Data: nil,
	}

	return mount.Create(ctx, relPath, obj)
}

// RemoveAll removes a directory and all its contents.
func (v *VirtualFileSystem) RemoveAll(ctx context.Context, path string) error {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return err
	}

	deleted, err := mount.Delete(ctx, relPath, true)
	if err != nil {
		return err
	}

	if !deleted {
		return ErrNotExist
	}

	return nil
}

// fileWriter implements io.WriteCloser for VFS files.
type fileWriter struct {
	mount  VirtualMount
	path   string
	ctx    context.Context
	buf    *bytes.Buffer
	closed bool
}

func (fw *fileWriter) Write(p []byte) (n int, err error) {
	if fw.closed {
		return 0, ErrClosed
	}
	return fw.buf.Write(p)
}

func (fw *fileWriter) Close() error {
	if fw.closed {
		return ErrClosed
	}
	fw.closed = true

	// Create the object
	obj := &VirtualObject{
		Info: VirtualObjectInfo{
			Path:    fw.path,
			Type:    ObjectTypeFile,
			Size:    int64(fw.buf.Len()),
			Mode:    0644,
			ModTime: time.Now(),
		},
		Data: bytes.NewReader(fw.buf.Bytes()),
	}

	// Check if file exists
	_, err := fw.mount.Stat(fw.ctx, fw.path)
	exists := err == nil

	if exists {
		// Update existing file
		_, err = fw.mount.Update(fw.ctx, fw.path, obj)
		return err
	}

	// Create new file
	return fw.mount.Create(fw.ctx, fw.path, obj)
}

// objectInfoToFileInfo converts VirtualObjectInfo to VirtualFileInfo.
func objectInfoToFileInfo(objInfo *VirtualObjectInfo) *VirtualFileInfo {
	return &VirtualFileInfo{
		Name:    objInfo.Name,
		Path:    objInfo.Path,
		Size:    objInfo.Size,
		Mode:    objInfo.Mode,
		IsDir:   objInfo.Type == ObjectTypeDirectory,
		ModTime: objInfo.ModTime,
	}
}
