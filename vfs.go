package vfs

import (
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

	return mount.Stat(ctx, relPath)
}

// ReadDir lists directory contents for the given path.
func (v *VirtualFileSystem) ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error) {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return nil, err
	}

	return mount.ReadDir(ctx, relPath)
}

// Open opens a file for reading.
func (v *VirtualFileSystem) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return nil, err
	}

	return mount.Open(ctx, relPath)
}

// Create creates a new file for writing.
func (v *VirtualFileSystem) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return nil, err
	}

	return mount.Create(ctx, relPath)
}

// Remove deletes a file.
func (v *VirtualFileSystem) Remove(ctx context.Context, path string) error {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return err
	}

	return mount.Remove(ctx, relPath)
}

// Mkdir creates a directory.
func (v *VirtualFileSystem) Mkdir(ctx context.Context, path string) error {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return err
	}

	return mount.Mkdir(ctx, relPath)
}

// RemoveAll removes a directory and all its contents.
func (v *VirtualFileSystem) RemoveAll(ctx context.Context, path string) error {
	mount, relPath, err := v.resolve(path)
	if err != nil {
		return err
	}

	return mount.RemoveAll(ctx, relPath)
}
