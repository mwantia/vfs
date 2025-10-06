package mounts

import (
	"bytes"
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

func (lm *LocalMount) GetCapabilities() vfs.VirtualMountCapabilities {
	return vfs.VirtualMountCapabilities{
		Capabilities: []vfs.VirtualMountCapability{
			vfs.VirtualMountCapabilityCRUD,
			vfs.VirtualMountCapabilityPermissions,
		},
	}
}

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

	// For files, return single entry
	if !info.IsDir() {
		return []*vfs.VirtualObjectInfo{
			lm.fileInfoToVirtual(info, path),
		}, nil
	}

	// For directories, return children
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, vfs.ErrNotExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return nil, vfs.ErrPermission
		}

		return nil, err
	}

	infos := make([]*vfs.VirtualObjectInfo, 0, len(entries))
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

func (lm *LocalMount) Get(ctx context.Context, path string) (*vfs.VirtualObject, error) {
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

	objInfo := lm.fileInfoToVirtual(info, path)

	// For directories, no data reader
	if info.IsDir() {
		return &vfs.VirtualObject{
			Info: *objInfo,
			Data: nil,
		}, nil
	}

	// For files, open for reading
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

	// Read entire file into memory
	data, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		return nil, err
	}

	return &vfs.VirtualObject{
		Info: *objInfo,
		Data: bytes.NewReader(data),
	}, nil
}

func (lm *LocalMount) Create(ctx context.Context, path string, obj *vfs.VirtualObject) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)

	// Check if already exists
	if _, err := os.Stat(fullPath); err == nil {
		return vfs.ErrExist
	}

	isDir := obj.Info.Type == vfs.ObjectTypeDirectory

	if isDir {
		// Create directory
		err := os.MkdirAll(fullPath, 0755)
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

	// Create parent directories if needed
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		if errors.Is(err, fs.ErrPermission) {
			return vfs.ErrPermission
		}

		return err
	}

	// Create file
	file, err := os.Create(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return vfs.ErrExist
		}

		if errors.Is(err, fs.ErrPermission) {
			return vfs.ErrPermission
		}

		return err
	}
	defer file.Close()

	// Write data if provided
	if obj.Data != nil {
		if _, err := io.Copy(file, obj.Data); err != nil {
			return err
		}
	}

	return nil
}

func (lm *LocalMount) Update(ctx context.Context, path string, obj *vfs.VirtualObject) (bool, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)

	// Check if exists
	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}

		if errors.Is(err, fs.ErrPermission) {
			return false, vfs.ErrPermission
		}

		return false, err
	}

	// Cannot change type
	isDir := obj.Info.Type == vfs.ObjectTypeDirectory
	if info.IsDir() != isDir {
		return false, fmt.Errorf("cannot change object type")
	}

	// For directories, nothing to update
	if isDir {
		return true, nil
	}

	// For files, update data if provided
	if obj.Data != nil {
		file, err := os.Create(fullPath)
		if err != nil {
			if errors.Is(err, fs.ErrPermission) {
				return false, vfs.ErrPermission
			}

			return false, err
		}
		defer file.Close()

		if _, err := io.Copy(file, obj.Data); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (lm *LocalMount) Delete(ctx context.Context, path string, force bool) (bool, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	fullPath := lm.resolvePath(path)

	// Check if exists
	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}

		if errors.Is(err, fs.ErrPermission) {
			return false, vfs.ErrPermission
		}

		return false, err
	}

	// If it's a directory
	if info.IsDir() {
		if force {
			// Remove all
			err = os.RemoveAll(fullPath)
		} else {
			// Check if empty
			entries, err := os.ReadDir(fullPath)
			if err != nil {
				return false, err
			}
			if len(entries) > 0 {
				return false, fmt.Errorf("directory not empty")
			}
			err = os.Remove(fullPath)
		}
	} else {
		// Remove file
		err = os.Remove(fullPath)
	}

	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}

		if errors.Is(err, fs.ErrPermission) {
			return false, vfs.ErrPermission
		}

		return false, err
	}

	return true, nil
}

func (lm *LocalMount) Upsert(ctx context.Context, path string, source any) error {
	// Check if exists
	_, err := lm.Stat(ctx, path)
	exists := err == nil

	// Convert source to VirtualObject
	obj, err := lm.sourceToObject(path, source)
	if err != nil {
		return err
	}

	if exists {
		_, err = lm.Update(ctx, path, obj)
		return err
	}

	return lm.Create(ctx, path, obj)
}

func (lm *LocalMount) sourceToObject(path string, source any) (*vfs.VirtualObject, error) {
	switch v := source.(type) {
	case *vfs.VirtualObject:
		return v, nil
	case []byte:
		return &vfs.VirtualObject{
			Info: vfs.VirtualObjectInfo{
				Path: path,
				Name: filepath.Base(path),
				Type: vfs.ObjectTypeFile,
				Size: int64(len(v)),
				Mode: 0644,
			},
			Data: bytes.NewReader(v),
		}, nil
	case io.Reader:
		data, err := io.ReadAll(v)
		if err != nil {
			return nil, err
		}
		return &vfs.VirtualObject{
			Info: vfs.VirtualObjectInfo{
				Path: path,
				Name: filepath.Base(path),
				Type: vfs.ObjectTypeFile,
				Size: int64(len(data)),
				Mode: 0644,
			},
			Data: bytes.NewReader(data),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported source type: %T", source)
	}
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
