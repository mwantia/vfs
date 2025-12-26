package direct

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

// NewDirectDriver creates a new Direct driver
func NewDirectDriver(uri *service.Uri) (*DirectDriver, error) {
	cfg := parseDirectBackendConfig(uri)

	return &DirectDriver{
		path: cfg.Path,
	}, nil
}

// Name returns the driver name
func (*DirectDriver) Name() string {
	return DriverName
}

// Health returns the health status of the driver by verifying the root directory is accessible
func (d *DirectDriver) Health() (bool, error) {
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
func (d *DirectDriver) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the driver is busy
	if !d.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	d.mu.Unlock()
	return false
}

// GetCapabilities returns the capabilities supported by this driver
func (*DirectDriver) GetCapabilities() *service.DriverCapabilities {
	return &service.DriverCapabilities{
		Type: DriverType,
		ObjectStorage: service.ObjectStorageCapabilities{
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
func (d *DirectDriver) OpenDriver(ctx context.Context) error {
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
func (d *DirectDriver) CloseDriver(ctx context.Context) error {
	// No cleanup needed - OS manages filesystem
	return nil
}

// GetObjectStorageService returns the ObjectStorage service
func (d *DirectDriver) GetObjectStorageService() service.ObjectStorageService {
	return &DirectObjectStorageService{
		driver: d,
	}
}

// GetMetadataService returns nil (Direct driver does not provide metadata service)
func (d *DirectDriver) GetMetadataService() service.MetadataService {
	return nil
}

// GetExtensionService returns nil (Direct driver does not support extensions)
func (d *DirectDriver) GetExtensionService(ext service.ServiceExtension) service.Service {
	return nil
}

// parseDirectBackendConfig parses URI into Direct configuration
func parseDirectBackendConfig(uri *service.Uri) *DirectBackendConfig {
	path := uri.Path

	// Handle Windows drive letters in host (file://C:/path → C:/path)
	if uri.Host != "" {
		path = uri.Host + ":" + path
	}

	// Clean and normalize the path
	path = filepath.Clean(path)

	return &DirectBackendConfig{
		Path: path,
	}
}
