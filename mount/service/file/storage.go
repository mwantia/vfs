package direct

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

// GetLifecycle returns the driver's lifecycle interface
func (s *DirectObjectStorageService) GetLifecycle() service.Lifecycle {
	return s.driver
}

// CreateObject creates a new file or directory in the filesystem
func (s *DirectObjectStorageService) CreateObject(ctx context.Context, key string, mode data.FileMode) (data.FileStat, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	fullPath := s.driver.resolvePath(key)

	// Check if exists
	if _, err := os.Stat(fullPath); err == nil {
		return data.FileStat{}, data.ErrExist
	}

	// Create directory or file
	if mode.IsDir() {
		if err := os.Mkdir(fullPath, 0755); err != nil {
			return data.FileStat{}, err
		}
	} else {
		file, err := os.Create(fullPath)
		if err != nil {
			return data.FileStat{}, err
		}
		defer file.Close()
	}

	// Stat the created object to get accurate metadata
	info, err := os.Stat(fullPath)
	if err != nil {
		return data.FileStat{}, err
	}

	return s.driver.toFileStat(key, info), nil
}

// ReadObject reads data from a file at the specified offset
func (s *DirectObjectStorageService) ReadObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	fullPath := s.driver.resolvePath(key)

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
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}

	// Read data
	n, err := file.Read(dat)
	if err != nil && err != io.EOF {
		return n, err
	}

	return n, err
}

// WriteObject writes data to a file at the specified offset
func (s *DirectObjectStorageService) WriteObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	fullPath := s.driver.resolvePath(key)

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
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}

	return file.Write(dat)
}

// DeleteObject deletes a file or directory
func (s *DirectObjectStorageService) DeleteObject(ctx context.Context, key string, force bool) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	fullPath := s.driver.resolvePath(key)

	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return data.ErrNotExist
		}
		return err
	}

	if info.IsDir() {
		// Directories ALWAYS require force=true (v2 convention)
		if !force {
			return data.ErrIsDirectory
		}
		return os.RemoveAll(fullPath)
	}

	return os.Remove(fullPath)
}

// ListObjects lists directory contents or returns info for a single file
func (s *DirectObjectStorageService) ListObjects(ctx context.Context, key string) ([]data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	fullPath := s.driver.resolvePath(key)

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

	// For files, return single entry
	if !info.IsDir() {
		return []data.FileStat{s.driver.toFileStat(key, info)}, nil
	}

	// For directories, list direct children
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	stats := make([]data.FileStat, 0, len(entries))
	for _, entry := range entries {
		childInfo, err := entry.Info()
		if err != nil {
			continue // Skip entries with errors
		}

		// Construct absolute key: parent path + basename
		absoluteKey := key
		if absoluteKey != "" {
			absoluteKey += "/"
		}
		absoluteKey += entry.Name()

		stats = append(stats, s.driver.toFileStat(absoluteKey, childInfo))
	}

	return stats, nil
}

// HeadObject returns metadata for a file or directory
func (s *DirectObjectStorageService) HeadObject(ctx context.Context, key string) (data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	fullPath := s.driver.resolvePath(key)

	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return data.FileStat{}, data.ErrNotExist
		}
		if errors.Is(err, fs.ErrPermission) {
			return data.FileStat{}, data.ErrPermission
		}

		return data.FileStat{}, err
	}

	return s.driver.toFileStat(key, info), nil
}

// TruncateObject resizes a file to the specified size
func (s *DirectObjectStorageService) TruncateObject(ctx context.Context, key string, size int64) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	fullPath := s.driver.resolvePath(key)
	return os.Truncate(fullPath, size)
}

// resolvePath joins the driver root with the relative key
func (d *DirectDriver) resolvePath(key string) string {
	// key comes WITHOUT leading slash from TraversalContext.RelativePath()
	return filepath.Join(d.path, filepath.Clean(key))
}

// toFileStat converts os.FileInfo to data.FileStat
func (d *DirectDriver) toFileStat(key string, info os.FileInfo) data.FileStat {
	// Convert os.FileMode to data.FileMode
	virtMode := data.FileMode(info.Mode().Perm())
	if info.IsDir() {
		virtMode |= data.ModeDir
	}

	return data.FileStat{
		Key:         key,
		Size:        info.Size(),
		Mode:        virtMode,
		ModifyTime:  info.ModTime(),
		CreateTime:  info.ModTime(), // OS doesn't always provide creation time
		ContentType: data.GetMIMEType(info.Name()),
	}
}
