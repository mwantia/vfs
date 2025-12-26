package mount

import (
	"context"
	"fmt"
	"time"

	"github.com/mwantia/vfs/mount/service"
)

type MountBuilder struct {
	ObjectStorage string                              `json:"object_storage"`
	Metadata      string                              `json:"metadata"`
	Extensions    map[service.ServiceExtension]string `json:"extensions"`
	Options       MountBuilderOptions                 `json:"options"`
}

type MountBuilderOptions struct {
	DynamicExtensions bool   `json:"dynamic_extensions"`
	Namespace         string `json:"namespace"`
	PathPrefix        string `json:"path_prefix"`
	IsReadOnly        bool   `json:"is_readonly"`
}

// BuildStep
type BuildStep func(*MountBuilder) error

// BuildMount
func BuildMounter(steps ...BuildStep) (*MountBuilder, error) {
	builder := &MountBuilder{
		Options: MountBuilderOptions{
			// Default value options
			DynamicExtensions: true,
			Namespace:         "",
			PathPrefix:        "",
			IsReadOnly:        false,
		},
		Extensions: make(map[service.ServiceExtension]string),
	}

	for _, step := range steps {
		if err := step(builder); err != nil {
			return nil, fmt.Errorf("failed to complete build step: %v", err)
		}
	}

	return builder, nil
}

func (mb *MountBuilder) Build(ctx context.Context, mountPoint string) (*Mount, error) {
	mnt := &Mount{
		MountPoint: mountPoint,
		Options: &MountOptions{
			Namespace:  mb.Options.Namespace,
			PathPrefix: mb.Options.PathPrefix,
			IsReadOnly: mb.Options.IsReadOnly,
		},
		Extensions: make(map[service.ServiceExtension]service.Service),
		uris:       []string{},
		fstab:      make(map[string]*Mount),
		createTime: time.Now(),
	}

	// Acquire ObjectStorage driver
	if mb.ObjectStorage != "" {
		d, err := service.AcquireDriver(ctx, mb.ObjectStorage)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire driver for object-storage: %w", err)
		}
		mnt.uris = append(mnt.uris, mb.ObjectStorage)

		caps := d.GetCapabilities()
		if caps.IsService(service.ServiceTypeMonolith) || caps.IsService(service.ServiceTypeObjectStorage) {
			mnt.ObjectStorage = d.GetObjectStorageService()
		}

		// Automatically load extensions declared by the ObjectStorage driver
		for _, ext := range caps.Extensions {
			// Skip if extension was manually specified with different URI
			if manualURI, exists := mb.Extensions[ext]; exists && manualURI != "" && manualURI != mb.ObjectStorage {
				continue
			}
			// Load extension from ObjectStorage driver
			if extService := d.GetExtensionService(ext); extService != nil {
				mnt.Extensions[ext] = extService
			}
		}
	}

	// Acquire Metadata driver
	if mb.Metadata != "" {
		d, err := service.AcquireDriver(ctx, mb.Metadata)
		if err != nil {
			// Rollback: release ObjectStorage driver
			for _, uri := range mnt.uris {
				service.ReleaseDriver(ctx, uri)
			}
			return nil, fmt.Errorf("failed to acquire driver for metadata: %w", err)
		}
		mnt.uris = append(mnt.uris, mb.Metadata)

		caps := d.GetCapabilities()
		if caps.IsService(service.ServiceTypeMonolith) || caps.IsService(service.ServiceTypeMetadata) {
			mnt.Metadata = d.GetMetadataService()
		}
	}

	// Acquire Extension drivers
	for ext, uri := range mb.Extensions {
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

func WithProfile(name string) BuildStep {
	return func(mb *MountBuilder) error {
		profile, err := GetProfile(name)
		if err != nil {
			return fmt.Errorf("failed to get profile: %w", err)
		}

		steps, err := profile.ToSteps()
		if err != nil {
			return fmt.Errorf("failed to parse profile steps: %w", err)
		}

		for _, step := range steps {
			if err := step(mb); err != nil {
				return fmt.Errorf("failed to complete build step: %v", err)
			}
		}

		return nil
	}
}

// WithObjectStorage
func WithObjectStorage(uri string) BuildStep {
	return func(mb *MountBuilder) error {
		if uri == "" {
			return fmt.Errorf("empty uri is not allowed")
		}

		mb.ObjectStorage = uri
		return nil
	}
}

// WithMetadata
func WithMetadata(uri string) BuildStep {
	return func(mb *MountBuilder) error {
		if uri == "" {
			return fmt.Errorf("empty uri is not allowed")
		}

		mb.Metadata = uri
		return nil
	}
}

// WithExtension
func WithExtension(ext service.ServiceExtension, uri string) BuildStep {
	return func(mb *MountBuilder) error {
		if uri == "" {
			return fmt.Errorf("empty uri is not allowed")
		}

		mb.Extensions[ext] = uri
		return nil
	}
}

// WithNamespace
func WithNamespace(namespace string) BuildStep {
	return func(mb *MountBuilder) error {
		mb.Options.Namespace = namespace
		return nil
	}
}

// DisableDynamicExtensions()
func DisableDynamicExtensions() BuildStep {
	return func(mb *MountBuilder) error {
		mb.Options.DynamicExtensions = false
		return nil
	}
}

// WithPathPrefix
func WithPathPrefix(pathPrefix string) BuildStep {
	return func(mb *MountBuilder) error {
		mb.Options.PathPrefix = pathPrefix
		return nil
	}
}

// AsReadOnly specifies, if this mount is in a readonly state.
func AsReadOnly() BuildStep {
	return func(mb *MountBuilder) error {
		mb.Options.IsReadOnly = true
		return nil
	}
}

// ToSpec converts a MountBuilder to a MountSpec.
// This is used when persisting mount configurations.
func (mb *MountBuilder) ToSpec() *MountSpec {
	spec := &MountSpec{
		ObjectStorage: mb.ObjectStorage,
		Metadata:      mb.Metadata,
		Extensions:    make(map[service.ServiceExtension]string),
		Options: &MountOptions{
			Namespace:  mb.Options.Namespace,
			PathPrefix: mb.Options.PathPrefix,
			IsReadOnly: mb.Options.IsReadOnly,
		},
	}

	// Convert extensions map
	for ext, uri := range mb.Extensions {
		spec.Extensions[ext] = uri
	}

	return spec
}
