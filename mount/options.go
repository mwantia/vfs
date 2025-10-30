package mount

import (
	"fmt"

	"github.com/mwantia/vfs/mount/backend"
)

type MountOptions struct {
	Backends map[backend.BackendCapability]backend.Backend

	Namespace      string
	PathPrefix     string
	AutoExtensions bool //
	CacheReads     bool // Cache file reads
	CacheWrites    bool // Buffer writes before upload
	IsReadOnly     bool // Whether the mount is read-only.
	AllowNesting   bool // Whether the mount allows for nested mountpoints.
}

type MountOption func(*MountOptions) error

// newDefaultMountOptions create a default set of mount options
func newDefaultMountOptions() *MountOptions {
	return &MountOptions{
		Backends:     make(map[backend.BackendCapability]backend.Backend),
		AllowNesting: true,
	}
}

// WithMetadata
func WithMetadata(metadata backend.MetadataBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilityMetadata] = metadata
		return nil
	}
}

// WithExtension
func WithExtension(ext backend.Backend, caps ...backend.BackendCapability) MountOption {
	return func(mo *MountOptions) error {
		for _, cap := range caps {
			if _, exists := mo.Backends[cap]; exists {
				return fmt.Errorf("capability already exists")
			}

			mo.Backends[cap] = ext
		}
		return nil
	}
}

// WithNamespace
func WithNamespace(namespace string) MountOption {
	return func(mo *MountOptions) error {
		mo.Namespace = namespace
		return nil
	}
}

// WithPathPrefix
func WithPathPrefix(pathPrefix string) MountOption {
	return func(mo *MountOptions) error {
		mo.PathPrefix = pathPrefix
		return nil
	}
}

// EnableAutoExtensions disables autosync, so the primary backend is only used as object-storage.
func EnableAutoExtensions() MountOption {
	return func(vmo *MountOptions) error {
		vmo.AutoExtensions = true
		return nil
	}
}

// WithCacheReads specifies, if file reads should be cached for this mount.
func WithCacheReads() MountOption {
	return func(vmo *MountOptions) error {
		vmo.CacheReads = true
		return nil
	}
}

// WithCacheWrites specifies, if write-operations will be buffered before upload.
func WithCacheWrites() MountOption {
	return func(vmo *MountOptions) error {
		vmo.CacheWrites = true
		return nil
	}
}

// WithDenyNesting specifies, if nested mountpoints are allowed within this mount.
func DisableMountNesting() MountOption {
	return func(vmo *MountOptions) error {
		vmo.AllowNesting = false
		return nil
	}
}

// IsReadOnly specifies, if this mount is in a readonly state.
func IsReadOnly() MountOption {
	return func(vmo *MountOptions) error {
		vmo.IsReadOnly = true
		return nil
	}
}
