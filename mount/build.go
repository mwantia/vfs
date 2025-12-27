package mount

import (
	"context"
	"fmt"
	"time"

	"github.com/mwantia/vfs/mount/builder"
	"github.com/mwantia/vfs/mount/service"
)

func IdentifyMountSteps(ctx context.Context, uri string) ([]builder.MountStep, error) {
	if uri == "" {
		return nil, fmt.Errorf("failed to parse uri")
	}

	driver, err := service.AcquireDriver(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire driver: %w", err)
	}
	// Release the driver after inspecting capabilities
	defer service.ReleaseDriver(ctx, uri)

	steps := make([]builder.MountStep, 0)

	caps := driver.GetCapabilities()
	if caps.IsService(service.ServiceTypeMonolith) || caps.IsService(service.ServiceTypeObjectStorage) {
		steps = append(steps, builder.WithObjectStorage(uri))
	}
	if caps.IsService(service.ServiceTypeMonolith) || caps.IsService(service.ServiceTypeMetadata) {
		steps = append(steps, builder.WithMetadata(uri))
	}
	for _, ext := range caps.Extensions {
		steps = append(steps, builder.WithExtension(ext, uri))
	}

	return steps, nil
}

func BuildMount(ctx context.Context, path string, build *builder.MountBuilder) (*Mount, error) {
	mnt := &Mount{
		MountPoint: path,
		Options: &MountOptions{
			Namespace:  build.Options.Namespace,
			PathPrefix: build.Options.PathPrefix,
			IsReadOnly: build.Options.IsReadOnly,
		},
		Extensions: make(map[service.ServiceExtension]service.Service),
		uris:       []string{},
		fstab:      make(map[string]*Mount),
		createTime: time.Now(),
	}

	// Acquire ObjectStorage driver
	if build.ObjectStorage != "" {
		d, err := service.AcquireDriver(ctx, build.ObjectStorage)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire driver for object-storage: %w", err)
		}
		mnt.uris = append(mnt.uris, build.ObjectStorage)

		caps := d.GetCapabilities()
		if caps.IsService(service.ServiceTypeMonolith) || caps.IsService(service.ServiceTypeObjectStorage) {
			mnt.ObjectStorage = d.GetObjectStorageService()
		}
	}

	// Acquire Metadata driver
	if build.Metadata != "" {
		d, err := service.AcquireDriver(ctx, build.Metadata)
		if err != nil {
			// Rollback: release ObjectStorage driver
			for _, uri := range mnt.uris {
				service.ReleaseDriver(ctx, uri)
			}
			return nil, fmt.Errorf("failed to acquire driver for metadata: %w", err)
		}
		mnt.uris = append(mnt.uris, build.Metadata)

		caps := d.GetCapabilities()
		if caps.IsService(service.ServiceTypeMonolith) || caps.IsService(service.ServiceTypeMetadata) {
			mnt.Metadata = d.GetMetadataService()
		}
	}

	// Acquire Extension drivers
	for ext, uri := range build.Extensions {
		if uri != "" {
			d, err := service.AcquireDriver(ctx, uri)
			if err != nil {
				// Rollback: release all acquired drivers
				for _, acquiredURI := range mnt.uris {
					service.ReleaseDriver(ctx, acquiredURI)
				}
				return nil, fmt.Errorf("failed to acquire driver for extension '%s': %w", ext, err)
			}
			mnt.uris = append(mnt.uris, uri)

			caps := d.GetCapabilities()
			if !caps.HasExtension(ext) {
				// Rollback: release all acquired drivers
				for _, acquiredURI := range mnt.uris {
					service.ReleaseDriver(ctx, acquiredURI)
				}
				return nil, fmt.Errorf("specified capabilities don't match for the extension")
			}

			mnt.Extensions[ext] = d.GetExtensionService(ext)
		}
	}

	return mnt, nil
}
