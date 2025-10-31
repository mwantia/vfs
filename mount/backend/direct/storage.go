package direct

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

func (db *DirectBackend) CreateObject(ctx context.Context, namespace, key string, mode data.FileMode) (*data.FileStat, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	fullPath := db.resolvePath(key)

	if _, err := os.Stat(fullPath); err == nil {
		return nil, data.ErrExist
	}

	if mode.IsDir() {
		return nil, os.Mkdir(fullPath, 0755)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	return &data.FileStat{
		Key:  key,
		Mode: mode,
		Size: 0,

		CreateTime: now,
		ModifyTime: now,
	}, file.Close()
}

func (db *DirectBackend) ReadObject(ctx context.Context, namespace, key string, offset int64, dat []byte) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	fullPath := db.resolvePath(key)

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

func (db *DirectBackend) WriteObject(ctx context.Context, namespace, key string, offset int64, dat []byte) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	fullPath := db.resolvePath(key)

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

func (db *DirectBackend) DeleteObject(ctx context.Context, namespace, key string, force bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	fullPath := db.resolvePath(key)

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

func (db *DirectBackend) ListObjects(ctx context.Context, namespace, key string) ([]*data.FileStat, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	fullPath := db.resolvePath(key)

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
		return []*data.FileStat{
			db.toFileStat(key, info),
		}, nil
	}

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	stats := make([]*data.FileStat, 0, len(entries))
	for _, entry := range entries {
		childInfo, err := entry.Info()
		if err != nil {
			continue
		}

		childKey := filepath.Join(key, entry.Name())
		stats = append(stats, db.toFileStat(childKey, childInfo))
	}

	return stats, nil
}

func (db *DirectBackend) HeadObject(ctx context.Context, namespace, key string) (*data.FileStat, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	fullPath := db.resolvePath(key)

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

	return db.toFileStat(key, info), nil
}

func (db *DirectBackend) TruncateObject(ctx context.Context, namespace, key string, size int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	fullPath := db.resolvePath(key)
	return os.Truncate(fullPath, size)
}
