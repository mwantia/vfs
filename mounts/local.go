package mounts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/mwantia/vfs"
)

// LocalMount provides access to the local filesystem.
// All operations are relative to the root path specified during creation.
// NOTE: This is currently a stub implementation - not fully functional.
type LocalMount struct {
	mu   sync.RWMutex
	root string
}

// NewLocal creates a new LocalMount rooted at the given path.
// The path must be an absolute path to an existing directory.
func NewLocal(root string) *LocalMount {
	return &LocalMount{
		root: filepath.Clean(root),
	}
}

// GetCapabilities returns the capabilities supported by this mount.
func (lm *LocalMount) GetCapabilities() vfs.VirtualMountCapabilities {
	return vfs.VirtualMountCapabilities{
		Capabilities: []vfs.VirtualMountCapability{
			vfs.VirtualMountCapabilityCRUD,
			vfs.VirtualMountCapabilityPermissions,
		},
	}
}

// Stat returns information about a file or directory on the local filesystem.
func (lm *LocalMount) Stat(ctx context.Context, path string) (*vfs.VirtualObjectInfo, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	fullPath := lm.resolvePath(path)
	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, vfs.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return nil, vfs.ErrPermission
		}
		return nil, err
	}

	return lm.fileInfoToVirtual(info, path), nil
}

// List returns all entries for a path.
func (lm *LocalMount) List(ctx context.Context, path string) ([]*vfs.VirtualObjectInfo, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	fullPath := lm.resolvePath(path)
	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, vfs.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return nil, vfs.ErrPermission
		}
		return nil, err
	}

	if !info.IsDir() {
		return []*vfs.VirtualObjectInfo{
			lm.fileInfoToVirtual(info, path),
		}, nil
	}

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	infos := make([]*vfs.VirtualObjectInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		childPath := filepath.Join(path, entry.Name())
		infos = append(infos, lm.fileInfoToVirtual(info, childPath))
	}

	return infos, nil
}

// Read reads data from a file at the given offset.
func (lm *LocalMount) Read(ctx context.Context, path string, offset int64, data []byte) (int, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	fullPath := lm.resolvePath(path)

	file, err := os.Open(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, vfs.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return 0, vfs.ErrPermission
		}
		return 0, err
	}
	defer file.Close()

	// Seek to offset
	if _, err := file.Seek(offset, 0); err != nil {
		return 0, err
	}

	// Read data
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		return n, err
	}
	return n, err
}

// Write writes data to a file at the given offset.
func (lm *LocalMount) Write(ctx context.Context, path string, offset int64, data []byte) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)

	// Open file for writing (O_RDWR to support both read and write)
	file, err := os.OpenFile(fullPath, os.O_RDWR, 0644)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, vfs.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return 0, vfs.ErrPermission
		}
		return 0, err
	}
	defer file.Close()

	// Seek to offset
	if _, err := file.Seek(offset, 0); err != nil {
		return 0, err
	}

	// Write data
	return file.Write(data)
}

// Create creates a new file or directory.
func (lm *LocalMount) Create(ctx context.Context, path string, isDir bool) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)

	if _, err := os.Stat(fullPath); err == nil {
		return vfs.ErrExist
	}

	if isDir {
		return os.Mkdir(fullPath, 0755)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	return file.Close()
}

// Delete removes a file or directory.
func (lm *LocalMount) Delete(ctx context.Context, path string, force bool) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)

	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return vfs.ErrNotExist
		}
		return err
	}

	if info.IsDir() {
		if force {
			return os.RemoveAll(fullPath)
		}
		entries, err := os.ReadDir(fullPath)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return fmt.Errorf("directory not empty")
		}
	}

	return os.Remove(fullPath)
}

// Truncate changes the size of a file.
func (lm *LocalMount) Truncate(ctx context.Context, path string, size int64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)
	return os.Truncate(fullPath, size)
}

// resolvePath joins the mount root with the relative path.
func (lm *LocalMount) resolvePath(path string) string {
	return filepath.Join(lm.root, filepath.Clean(path))
}

// fileInfoToVirtual converts os.FileInfo to VirtualObjectInfo.
func (lm *LocalMount) fileInfoToVirtual(info fs.FileInfo, path string) *vfs.VirtualObjectInfo {
	objType := vfs.ObjectTypeFile
	if info.IsDir() {
		objType = vfs.ObjectTypeDirectory
	}

	mode := vfs.VirtualFileMode(info.Mode().Perm())
	if info.IsDir() {
		mode |= vfs.ModeDir
	}

	return &vfs.VirtualObjectInfo{
		Path:    path,
		Name:    info.Name(),
		Type:    objType,
		Size:    info.Size(),
		Mode:    mode,
		ModTime: info.ModTime(),
	}
}
