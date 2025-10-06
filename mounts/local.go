package mounts

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/mwantia/vfs"
)

// LocalMount provides access to the local filesystem.
// All operations are relative to the root path specified during creation.
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

// resolvePath joins the mount root with the relative path and cleans it.
func (lm *LocalMount) resolvePath(path string) string {
	return filepath.Join(lm.root, filepath.Clean(path))
}

// Stat returns file information for the given path.
func (lm *LocalMount) Stat(ctx context.Context, path string) (*vfs.VirtualFileInfo, error) {
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

// ReadDir lists directory contents for the given path.
func (lm *LocalMount) ReadDir(ctx context.Context, path string) ([]*vfs.VirtualFileInfo, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	fullPath := lm.resolvePath(path)
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, vfs.ErrNotExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return nil, vfs.ErrPermission
		}

		info, statErr := os.Stat(fullPath)
		if statErr == nil && !info.IsDir() {
			return nil, vfs.ErrNotDirectory
		}

		return nil, err
	}

	infos := make([]*vfs.VirtualFileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue // Skip entries we can't read
		}

		childPath := filepath.Join(path, entry.Name())
		infos = append(infos, lm.fileInfoToVirtual(info, childPath))
	}

	return infos, nil
}

// Open opens a file for reading.
func (lm *LocalMount) Open(ctx context.Context, path string) (io.ReadCloser, error) {
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

	if info.IsDir() {
		return nil, vfs.ErrIsDirectory
	}

	file, err := os.Open(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, vfs.ErrNotExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return nil, vfs.ErrPermission
		}

		return nil, err
	}

	return file, nil
}

// Create creates a new file for writing.
func (lm *LocalMount) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)
	info, err := os.Stat(fullPath)
	if err == nil && info.IsDir() {
		return nil, vfs.ErrIsDirectory
	}

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		if errors.Is(err, fs.ErrPermission) {
			return nil, vfs.ErrPermission
		}

		return nil, err
	}

	file, err := os.Create(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, vfs.ErrExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return nil, vfs.ErrPermission
		}

		return nil, err
	}

	return file, nil
}

// Remove deletes a file.
func (lm *LocalMount) Remove(ctx context.Context, path string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)
	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return vfs.ErrNotExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return vfs.ErrPermission
		}

		return err
	}

	if info.IsDir() {
		return vfs.ErrIsDirectory
	}

	err = os.Remove(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return vfs.ErrNotExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return vfs.ErrPermission
		}

		return err
	}

	return nil
}

// Mkdir creates a directory.
func (lm *LocalMount) Mkdir(ctx context.Context, path string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)
	err := os.Mkdir(fullPath, 0755)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return vfs.ErrExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return vfs.ErrPermission
		}

		return err
	}

	return nil
}

// RemoveAll removes a directory and all its contents.
func (lm *LocalMount) RemoveAll(ctx context.Context, path string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)
	if _, err := os.Stat(fullPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return vfs.ErrNotExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return vfs.ErrPermission
		}

		return err
	}

	err := os.RemoveAll(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrPermission) {
			return vfs.ErrPermission
		}

		return err
	}

	return nil
}

// fileInfoToVirtual converts os.FileInfo to VirtualFileInfo.
func (lm *LocalMount) fileInfoToVirtual(info fs.FileInfo, path string) *vfs.VirtualFileInfo {
	mode := vfs.VirtualFileMode(info.Mode().Perm())
	if info.IsDir() {
		mode |= vfs.ModeDir
	}

	return &vfs.VirtualFileInfo{
		Name:    info.Name(),
		Path:    path,
		Size:    info.Size(),
		Mode:    mode,
		IsDir:   info.IsDir(),
		ModTime: info.ModTime(),
	}
}
