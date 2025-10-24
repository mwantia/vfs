package local

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/mwantia/vfs/data"
)

func (lb *LocalBackend) CreateObject(ctx context.Context, key string, fileType data.VirtualFileType, fileMode data.VirtualFileMode) (*data.VirtualFileStat, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	fullPath := lb.resolvePath(key)

	if _, err := os.Stat(fullPath); err == nil {
		return nil, data.ErrExist
	}

	if fileMode.IsDir() {
		return nil, os.Mkdir(fullPath, 0755)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	return &data.VirtualFileStat{
		Key:  key,
		Type: fileType,
		Mode: fileMode,
		Size: 0,

		CreateTime: now,
		ModifyTime: now,
	}, file.Close()
}

func (lb *LocalBackend) ReadObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	fullPath := lb.resolvePath(key)

	file, err := os.Open(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, data.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return 0, data.ErrPermission
		}

		return 0, err
	}
	defer file.Close()

	// Seek to offset
	if _, err := file.Seek(offset, 0); err != nil {
		return 0, err
	}

	// Read data
	n, err := file.Read(dat)
	if err != nil && err != io.EOF {
		return n, err
	}

	return n, err
}

func (lb *LocalBackend) WriteObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	fullPath := lb.resolvePath(key)

	// Open file for writing (O_RDWR to support both read and write)
	file, err := os.OpenFile(fullPath, os.O_RDWR, 0644)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, data.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return 0, data.ErrPermission
		}

		return 0, err
	}
	defer file.Close()

	// Seek to offset
	if _, err := file.Seek(offset, 0); err != nil {
		return 0, err
	}

	return file.Write(dat)
}

func (lb *LocalBackend) DeleteObject(ctx context.Context, key string, force bool) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	fullPath := lb.resolvePath(key)

	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return data.ErrNotExist
		}
		return err
	}

	if info.IsDir() {
		if !force {
			// Directories require force=true to delete
			return data.ErrIsDirectory
		}

		return os.RemoveAll(fullPath)
	}

	return os.Remove(fullPath)
}

func (lb *LocalBackend) ListObjects(ctx context.Context, key string) ([]*data.VirtualFileStat, error) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	fullPath := lb.resolvePath(key)

	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, data.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return nil, data.ErrPermission
		}
		return nil, err
	}

	if !info.IsDir() {
		return []*data.VirtualFileStat{
			lb.toVirtualFileStat(key, info),
		}, nil
	}

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	stats := make([]*data.VirtualFileStat, 0, len(entries))
	for _, entry := range entries {
		childInfo, err := entry.Info()
		if err != nil {
			continue
		}

		childKey := filepath.Join(key, entry.Name())
		stats = append(stats, lb.toVirtualFileStat(childKey, childInfo))
	}

	return stats, nil
}

func (lb *LocalBackend) HeadObject(ctx context.Context, key string) (*data.VirtualFileStat, error) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	fullPath := lb.resolvePath(key)

	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, data.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return nil, data.ErrPermission
		}

		return nil, err
	}

	return lb.toVirtualFileStat(key, info), nil
}

func (lb *LocalBackend) TruncateObject(ctx context.Context, key string, size int64) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	fullPath := lb.resolvePath(key)
	return os.Truncate(fullPath, size)
}
