package ephemeral

import (
	"context"
	"fmt"
	"os"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/extensions/notification"
	"github.com/mwantia/vfs/mount/service"
	"github.com/tidwall/btree"
)

// NewEphemeralMonolithDriver
func NewEphemeralMonolithDriver(uri *service.Uri) *EphemeralMonolithDriver {
	return &EphemeralMonolithDriver{
		keys:     btree.NewMap[string, string](0),
		dirs:     make(map[string][]string),
		metadata: make(map[string]data.Metadata),
		refCount: make(map[string]int),

		stats: make(map[string]data.FileStat),
		datas: make(map[string][]byte),
	}
}

// Name
func (*EphemeralMonolithDriver) Name() string {
	return DriverName
}

// Health returns the health status of the driver
func (*EphemeralMonolithDriver) Health() (bool, error) {
	return true, nil
}

// IsBusy checks if the driver is currently busy with operations
func (d *EphemeralMonolithDriver) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the driver is busy
	if !d.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	d.mu.Unlock()
	return false
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (*EphemeralMonolithDriver) GetCapabilities() *service.DriverCapabilities {
	return &service.DriverCapabilities{
		Type: DriverType,
		Extensions: []service.ServiceExtension{
			service.ServiceExtensionMount,
			service.ServiceExtensionNotification,
		},
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
				MaxObjectSize: 10485760, // 10 MB for ephemeral
				MaxPathLength: 255,
				CaseSensitive: true,
			},
		},
	}
}

// OpenDriver
func (*EphemeralMonolithDriver) OpenDriver(ctx context.Context) error {
	return nil
}

// CloseDriver
func (d *EphemeralMonolithDriver) CloseDriver(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Clear btree
	d.keys.Clear()
	// Clear stored metadata
	for k := range d.metadata {
		delete(d.metadata, k)
	}
	// Clear stored directories
	for k := range d.dirs {
		delete(d.dirs, k)
	}
	// Clear reference counts
	for k := range d.refCount {
		delete(d.refCount, k)
	}

	// Clear stored stats
	for k := range d.stats {
		delete(d.stats, k)
	}
	// Clear stored data
	for k := range d.datas {
		delete(d.datas, k)
	}

	return nil
}

// GetObjectStorageService
func (d *EphemeralMonolithDriver) GetObjectStorageService() service.ObjectStorageService {
	return &EphemeralObjectStorageService{
		driver: d,
	}
}

// GetMetadataService
func (d *EphemeralMonolithDriver) GetMetadataService() service.MetadataService {
	return &EphemeralMetadataService{
		driver: d,
	}
}

// GetExtensionService
func (d *EphemeralMonolithDriver) GetExtensionService(ext service.ServiceExtension) service.Service {
	if ext == service.ServiceExtensionMount {
		return &EphemeralMountExtensionService{
			driver: d,
		}
	}
	if ext == service.ServiceExtensionNotification {
		service := &EphemeralNotificationExtensionService{
			driver: d,
			hub: &EphemeralNotificationHub{
				subscriptions: make(map[string]*ephemeralSubscription),
				events:        make([]notification.NotificationEvent, 0),
			},
		}
		service.Subscribe(context.Background(), "/gosync", func(ctx context.Context, event notification.NotificationEvent) error {
			f, _ := os.OpenFile(
				"out.log",
				os.O_APPEND|os.O_CREATE|os.O_WRONLY,
				0o644,
			)
			defer f.Close()

			fmt.Fprintf(f, "Change: %s at %s\n", event.Type, event.AbsolutePath)
			return nil
		})

		return service
	}

	return nil
}
