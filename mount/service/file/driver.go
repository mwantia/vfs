package file

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

// NewFileDriver creates a new File driver
func NewFileDriver(uri *service.Uri) (*FileDriver, error) {
	cfg := parseFileBackendConfig(uri)

	return &FileDriver{
		path: cfg.Path,
	}, nil
}

// Name returns the driver name
func (*FileDriver) Name() string {
	return DriverName
}

// Health returns the health status of the driver by verifying the root directory is accessible
func (d *FileDriver) Health() (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Check if root directory exists and is accessible
	info, err := os.Stat(d.path)
	if err != nil {
		return false, err
	}

	if !info.IsDir() {
		return false, fmt.Errorf("path is not a directory: %s", d.path)
	}

	return true, nil
}

// IsBusy checks if the driver is currently busy with operations
func (d *FileDriver) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the driver is busy
	if !d.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	d.mu.Unlock()
	return false
}

// GetCapabilities returns the capabilities supported by this driver
func (*FileDriver) GetCapabilities() *service.DriverCapabilities {
	return &service.DriverCapabilities{
		Type: DriverType,
		ObjectStorage: service.ObjectStorageCapabilities{
			BufferSize: 5 * 1024 * 1024, // 5 MB
			Operations: []service.ObjectStorageOperation{
				service.ObjectStorageOperationCreate,
				service.ObjectStorageOperationRead,
				service.ObjectStorageOperationWrite,
				service.ObjectStorageOperationDelete,
				service.ObjectStorageOperationList,
				service.ObjectStorageOperationStream,
			},
			AccessModes: []service.ObjectStorageAccessMode{
				service.ObjectStorageAccessReadWrite,
			},
			Constraints: service.ObjectStorageConstraints{
				MaxObjectSize: 10737418240, // 10 GB
				MaxPathLength: 4096,        // Typical filesystem limit
				MaxPathDepth:  255,
				CaseSensitive: true, // Unix-like default
			},
		},
	}
}

// OpenDriver initializes the driver and verifies the root directory
func (d *FileDriver) OpenDriver(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Verify the root directory exists
	info, err := os.Stat(d.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("mount path does not exist: %s", d.path)
		}
		if errors.Is(err, fs.ErrPermission) {
			return data.ErrPermission
		}
		return err
	}

	// Ensure the root is a directory
	if !info.IsDir() {
		return data.ErrNotDirectory
	}

	return nil
}

// CloseDriver cleans up resources (no-op for filesystem driver)
func (d *FileDriver) CloseDriver(ctx context.Context) error {
	// No cleanup needed - OS manages filesystem
	return nil
}

// GetObjectStorageService returns the ObjectStorage service
func (d *FileDriver) GetObjectStorageService() service.ObjectStorageService {
	return &FileObjectStorageService{
		driver: d,
	}
}

// GetMetadataService returns nil (Direct driver does not provide metadata service)
func (d *FileDriver) GetMetadataService() service.MetadataService {
	return nil
}

// GetExtensionService returns nil (Direct driver does not support extensions)
func (d *FileDriver) GetExtensionService(ext service.ServiceExtension) service.Service {
	return nil
}

// parseDirectBackendConfig parses URI into Direct configuration
func parseFileBackendConfig(uri *service.Uri) *FileBackendConfig {
	path := uri.Path

	// Handle Windows drive letters in host (file://C:/path → C:/path)
	if uri.Host != "" {
		path = uri.Host + ":" + path
	}

	// Clean and normalize the path
	path = filepath.Clean(path)

	return &FileBackendConfig{
		Path: path,
	}
}
