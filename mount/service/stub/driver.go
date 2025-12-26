package stub

import (
	"context"

	"github.com/mwantia/vfs/mount/service"
)

// Name
func (*StubObjectStorageDriver) Name() string {
	return DriverName
}

// Health returns the health status of the driver
func (*StubObjectStorageDriver) Health() (bool, error) {
	return true, nil
}

// IsBusy checks if the driver is currently busy with operations
func (*StubObjectStorageDriver) IsBusy() bool {
	return false
}

// GetDriverCapabilities returns a list of capabilities supported by this backend.
func (*StubObjectStorageDriver) GetCapabilities() *service.DriverCapabilities {
	return &service.DriverCapabilities{
		Type: DriverType,
		ObjectStorage: service.ObjectStorageCapabilities{
			Operations:  []service.ObjectStorageOperation{}, // No operations supported - read-only stub
			AccessModes: []service.ObjectStorageAccessMode{},
			Constraints: service.ObjectStorageConstraints{
				IsReadonly:    true,
				CaseSensitive: false,
				MaxPathLength: 255,
			},
		},
	}
}

// OpenDriver
func (*StubObjectStorageDriver) OpenDriver(ctx context.Context) error {
	return nil
}

// CloseDriver
func (*StubObjectStorageDriver) CloseDriver(ctx context.Context) error {
	return nil
}

// GetObjectStorageService
func (d *StubObjectStorageDriver) GetObjectStorageService() service.ObjectStorageService {
	return d
}

// GetMetadataService
func (*StubObjectStorageDriver) GetMetadataService() service.MetadataService {
	return nil
}

// GetExtensionService
func (*StubObjectStorageDriver) GetExtensionService(ext service.ServiceExtension) service.Service {
	return nil
}
