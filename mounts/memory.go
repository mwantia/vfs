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
	data    []byte
	size    int64
	isDir   bool
	modTime time.Time
}

func NewMemory() *MemoryMount {
	memory := &MemoryMount{
		files: make(map[string]*MemoryFile),
	}
	memory.files[""] = &MemoryFile{
		data:    make([]byte, 0),
		size:    0,
		isDir:   true,
		modTime: time.Now(),
	}

	return memory
}

func (m *MemoryMount) Stat(ctx context.Context, p string) (*vfs.VirtualFileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	file, exists := m.files[p]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	mode := vfs.VirtualFileMode(0644)
	if file.isDir {
		mode = vfs.ModeDir | 0755
	}

	return &vfs.VirtualFileInfo{
		Name:    path.Base(p),
		Path:    p,
		Size:    file.size,
		Mode:    mode,
		IsDir:   file.isDir,
		ModTime: file.modTime,
	}, nil
}

func (m *MemoryMount) ReadDir(ctx context.Context, p string) ([]*vfs.VirtualFileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dir, exists := m.files[p]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	if !dir.isDir {
		return nil, vfs.ErrNotDirectory
	}

	var files []*vfs.VirtualFileInfo
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

		mode := vfs.VirtualFileMode(0644)
		size := file.size

		if isChildDir {
			mode = vfs.ModeDir | 0755
			size = 0
		}

		files = append(files, &vfs.VirtualFileInfo{
			Name:    childName,
			Path:    childPath,
			Size:    size,
			Mode:    mode,
			IsDir:   isChildDir,
			ModTime: file.modTime,
		})
	}

	return files, nil
}

func (m *MemoryMount) Open(ctx context.Context, p string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	file, exists := m.files[p]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	if file.isDir {
		return nil, vfs.ErrIsDirectory
	}

	data := make([]byte, len(file.data))
	copy(data, file.data)

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MemoryMount) Create(ctx context.Context, p string) (io.WriteCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if file, exists := m.files[p]; exists && file.isDir {
		return nil, vfs.ErrIsDirectory
	}

	if err := m.mkdirAllLocked(path.Dir(p)); err != nil {
		return nil, err
	}

	// Create or truncate file
	m.files[p] = &MemoryFile{
		data:    nil,
		isDir:   false,
		modTime: time.Now(),
	}

	return &MemoryFileWriter{
		memory: m,
		path:   p,
		buf:    new(bytes.Buffer),
	}, nil
}

func (m *MemoryMount) Remove(ctx context.Context, p string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, exists := m.files[p]
	if !exists {
		return vfs.ErrNotExist
	}

	if file.isDir {
		return vfs.ErrIsDirectory
	}

	delete(m.files, p)
	return nil
}

func (m *MemoryMount) Mkdir(ctx context.Context, p string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, exists := m.files[p]; exists {
		return vfs.ErrExist
	}

	m.files[p] = &MemoryFile{
		data:    make([]byte, 0),
		size:    0,
		isDir:   true,
		modTime: time.Now(),
	}

	return nil
}

func (m *MemoryMount) RemoveAll(ctx context.Context, p string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, exists := m.files[p]; !exists {
		return vfs.ErrNotExist
	}

	prefix := p
	if prefix != "" {
		prefix += "/"
	}

	for filePath := range m.files {
		if filePath == p || strings.HasPrefix(filePath, prefix) {
			delete(m.files, filePath)
		}
	}

	return nil
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
		isDir:   true,
		modTime: time.Now(),
	}

	return nil
}

// memFileWriter implements io.WriteCloser for in-memory files.
type MemoryFileWriter struct {
	memory *MemoryMount
	path   string
	buf    *bytes.Buffer
	closed bool
}

func (mfw *MemoryFileWriter) Write(p []byte) (n int, err error) {
	if mfw.closed {
		return 0, vfs.ErrClosed
	}

	file, exists := mfw.memory.files[mfw.path]
	if !exists {
		return 0, vfs.ErrClosed
	}

	offset, err := mfw.buf.Write(p)
	if err != nil {
		return 0, err
	}
	file.size += int64(offset)

	return offset, nil
}

func (mfw *MemoryFileWriter) Close() error {
	if mfw.closed {
		return vfs.ErrClosed
	}
	mfw.closed = true

	mfw.memory.mu.Lock()
	defer mfw.memory.mu.Unlock()

	// Update file data
	if file, exists := mfw.memory.files[mfw.path]; exists {
		file.data = mfw.buf.Bytes()
		file.modTime = time.Now()
	}

	return nil
}
