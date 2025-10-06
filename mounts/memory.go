package mounts

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mwantia/vfs"
)

type MemoryMount struct {
	mu    sync.RWMutex
	files map[string]*MemoryFile
}

type MemoryFile struct {
	data     []byte
	size     int64
	isDir    bool
	modTime  time.Time
	metadata map[string]string
}

func NewMemory() *MemoryMount {
	memory := &MemoryMount{
		files: make(map[string]*MemoryFile),
	}
	memory.files[""] = &MemoryFile{
		data:     make([]byte, 0),
		size:     0,
		isDir:    true,
		modTime:  time.Now(),
		metadata: make(map[string]string),
	}

	return memory
}

func (m *MemoryMount) GetCapabilities() vfs.VirtualMountCapabilities {
	return vfs.VirtualMountCapabilities{
		Capabilities: []vfs.VirtualMountCapability{
			vfs.VirtualMountCapabilityCRUD,
			vfs.VirtualMountCapabilityMetadata,
		},
	}
}

func (m *MemoryMount) Stat(ctx context.Context, p string) (*vfs.VirtualObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	file, exists := m.files[p]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	objType := vfs.ObjectTypeFile
	if file.isDir {
		objType = vfs.ObjectTypeDirectory
	}

	mode := vfs.VirtualFileMode(0644)
	if file.isDir {
		mode = vfs.ModeDir | 0755
	}

	return &vfs.VirtualObjectInfo{
		Path:     p,
		Name:     path.Base(p),
		Type:     objType,
		Size:     file.size,
		Mode:     mode,
		ModTime:  file.modTime,
		Metadata: copyMetadata(file.metadata),
	}, nil
}

func (m *MemoryMount) List(ctx context.Context, p string) ([]*vfs.VirtualObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dir, exists := m.files[p]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	// For files, return single entry
	if !dir.isDir {
		mode := vfs.VirtualFileMode(0644)
		return []*vfs.VirtualObjectInfo{
			{
				Path:     p,
				Name:     path.Base(p),
				Type:     vfs.ObjectTypeFile,
				Size:     dir.size,
				Mode:     mode,
				ModTime:  dir.modTime,
				Metadata: copyMetadata(dir.metadata),
			},
		}, nil
	}

	// For directories, return children
	var objects []*vfs.VirtualObjectInfo
	prefix := p
	if prefix != "" {
		prefix += "/"
	}

	seen := make(map[string]bool)
	for filePath, file := range m.files {
		if strings.EqualFold(filePath, p) {
			continue
		}

		if !strings.HasPrefix(filePath, prefix) {
			continue
		}

		rel := strings.TrimPrefix(filePath, prefix)
		parts := strings.Split(rel, "/")
		if len(parts) == 0 {
			continue
		}

		childName := parts[0]
		if seen[childName] {
			continue
		}
		seen[childName] = true

		childPath := prefix + childName
		isChildDir := file.isDir
		if len(parts) > 1 {
			isChildDir = true
		}

		objType := vfs.ObjectTypeFile
		mode := vfs.VirtualFileMode(0644)
		size := file.size

		if isChildDir {
			objType = vfs.ObjectTypeDirectory
			mode = vfs.ModeDir | 0755
			size = 0
		}

		objects = append(objects, &vfs.VirtualObjectInfo{
			Path:     childPath,
			Name:     childName,
			Type:     objType,
			Size:     size,
			Mode:     mode,
			ModTime:  file.modTime,
			Metadata: copyMetadata(file.metadata),
		})
	}

	return objects, nil
}

func (m *MemoryMount) Get(ctx context.Context, p string) (*vfs.VirtualObject, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	file, exists := m.files[p]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	objType := vfs.ObjectTypeFile
	if file.isDir {
		objType = vfs.ObjectTypeDirectory
	}

	mode := vfs.VirtualFileMode(0644)
	if file.isDir {
		mode = vfs.ModeDir | 0755
	}

	info := vfs.VirtualObjectInfo{
		Path:     p,
		Name:     path.Base(p),
		Type:     objType,
		Size:     file.size,
		Mode:     mode,
		ModTime:  file.modTime,
		Metadata: copyMetadata(file.metadata),
	}

	// For directories, no data reader
	if file.isDir {
		return &vfs.VirtualObject{
			Info: info,
			Data: nil,
		}, nil
	}

	// For files, copy data to reader
	data := make([]byte, len(file.data))
	copy(data, file.data)

	return &vfs.VirtualObject{
		Info: info,
		Data: bytes.NewReader(data),
	}, nil
}

func (m *MemoryMount) Create(ctx context.Context, p string, obj *vfs.VirtualObject) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.files[p]; exists {
		return vfs.ErrExist
	}

	// Create parent directories if needed
	if err := m.mkdirAllLocked(path.Dir(p)); err != nil {
		return err
	}

	isDir := obj.Info.Type == vfs.ObjectTypeDirectory
	var data []byte
	var size int64

	if !isDir && obj.Data != nil {
		var err error
		data, err = io.ReadAll(obj.Data)
		if err != nil {
			return err
		}
		size = int64(len(data))
	}

	metadata := make(map[string]string)
	if obj.Info.Metadata != nil {
		metadata = copyMetadata(obj.Info.Metadata)
	}

	m.files[p] = &MemoryFile{
		data:     data,
		size:     size,
		isDir:    isDir,
		modTime:  time.Now(),
		metadata: metadata,
	}

	return nil
}

func (m *MemoryMount) Update(ctx context.Context, p string, obj *vfs.VirtualObject) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[p]
	if !exists {
		return false, nil
	}

	// Cannot change type
	isDir := obj.Info.Type == vfs.ObjectTypeDirectory
	if file.isDir != isDir {
		return false, fmt.Errorf("cannot change object type")
	}

	// Update data if provided and not a directory
	if !isDir && obj.Data != nil {
		data, err := io.ReadAll(obj.Data)
		if err != nil {
			return false, err
		}
		file.data = data
		file.size = int64(len(data))
	}

	// Update metadata
	if obj.Info.Metadata != nil {
		file.metadata = copyMetadata(obj.Info.Metadata)
	}

	file.modTime = time.Now()

	return true, nil
}

func (m *MemoryMount) Delete(ctx context.Context, p string, force bool) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[p]
	if !exists {
		return false, nil
	}

	// If it's a directory and force is false, check if it has children
	if file.isDir && !force {
		prefix := p
		if prefix != "" {
			prefix += "/"
		}

		for filePath := range m.files {
			if filePath != p && strings.HasPrefix(filePath, prefix) {
				return false, fmt.Errorf("directory not empty")
			}
		}
	}

	// Remove file or directory
	if force && file.isDir {
		// Remove all children
		prefix := p
		if prefix != "" {
			prefix += "/"
		}

		for filePath := range m.files {
			if filePath == p || strings.HasPrefix(filePath, prefix) {
				delete(m.files, filePath)
			}
		}
	} else {
		delete(m.files, p)
	}

	return true, nil
}

func (m *MemoryMount) Upsert(ctx context.Context, p string, source any) error {
	// Check if exists
	_, err := m.Stat(ctx, p)
	exists := err == nil

	// Convert source to VirtualObject
	obj, err := m.sourceToObject(p, source)
	if err != nil {
		return err
	}

	if exists {
		_, err = m.Update(ctx, p, obj)
		return err
	}

	return m.Create(ctx, p, obj)
}

func (m *MemoryMount) sourceToObject(p string, source any) (*vfs.VirtualObject, error) {
	switch v := source.(type) {
	case *vfs.VirtualObject:
		return v, nil
	case []byte:
		return &vfs.VirtualObject{
			Info: vfs.VirtualObjectInfo{
				Path:    p,
				Name:    path.Base(p),
				Type:    vfs.ObjectTypeFile,
				Size:    int64(len(v)),
				Mode:    0644,
				ModTime: time.Now(),
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
				Path:    p,
				Name:    path.Base(p),
				Type:    vfs.ObjectTypeFile,
				Size:    int64(len(data)),
				Mode:    0644,
				ModTime: time.Now(),
			},
			Data: bytes.NewReader(data),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported source type: %T", source)
	}
}

func (m *MemoryMount) mkdirAllLocked(p string) error {
	if p == "" || p == "." {
		return nil
	}

	// Check if already exists
	if file, exists := m.files[p]; exists {
		if !file.isDir {
			return fmt.Errorf("%w: %s", vfs.ErrNotDirectory, p)
		}
		return nil
	}

	// Create parent first
	parent := path.Dir(p)
	if parent != "." && parent != p {
		if err := m.mkdirAllLocked(parent); err != nil {
			return err
		}
	}

	// Create this directory
	m.files[p] = &MemoryFile{
		isDir:    true,
		modTime:  time.Now(),
		metadata: make(map[string]string),
	}

	return nil
}

func copyMetadata(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
