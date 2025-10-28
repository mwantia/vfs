package mount

import (
	"github.com/mwantia/vfs/mount/backend"
	"github.com/mwantia/vfs/mount/extension/acl"
	"github.com/mwantia/vfs/mount/extension/cache"
	"github.com/mwantia/vfs/mount/extension/encrypt"
	"github.com/mwantia/vfs/mount/extension/multipart"
	"github.com/mwantia/vfs/mount/extension/rubbish"
	"github.com/mwantia/vfs/mount/extension/snapshot"
	"github.com/mwantia/vfs/mount/extension/versioning"
)

type MountOptions struct {
	Backends map[backend.VirtualBackendCapability]backend.VirtualBackend

	Auto        bool //
	CacheReads  bool // Cache file reads
	CacheWrites bool // Buffer writes before upload
	ReadOnly    bool // Whether the mount is read-only.
	Nesting     bool // Whether the mount allows for nested mountpoints.
}

type MountOption func(*MountOptions) error

func newDefaultMountOptions() *MountOptions {
	return &MountOptions{
		Backends:    make(map[backend.VirtualBackendCapability]backend.VirtualBackend),
		Auto:        true,
		CacheReads:  false,
		CacheWrites: false,
		ReadOnly:    false,
		Nesting:     true,
	}
}

func WithACL(ext acl.VirtualAclBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilityACL] = ext
		return nil
	}
}

func WithCache(ext cache.VirtualCacheBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilityCache] = ext
		return nil
	}
}

func WithEncrypt(ext encrypt.VirtualEncryptBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilityEncrypt] = ext
		return nil
	}
}

func WithMetadata(ext backend.VirtualMetadataBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilityMetadata] = ext
		return nil
	}
}

func WithMultipart(ext multipart.VirtualMultipartBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilityMultipart] = ext
		return nil
	}
}

func WithRubbish(ext rubbish.VirtualRubbishBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilityRubbish] = ext
		return nil
	}
}

func WithSnapshot(ext snapshot.VirtualSnapshotBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilitySnapshot] = ext
		return nil
	}
}

func WithVersioning(ext versioning.VirtualVersioningBackend) MountOption {
	return func(vmo *MountOptions) error {
		vmo.Backends[backend.CapabilityVersioning] = ext
		return nil
	}
}

// DisableAuto disables autosync, so the primary backend is only used as object-storage.
func DisableAuto() MountOption {
	return func(vmo *MountOptions) error {
		vmo.Auto = false
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
func DisableNesting() MountOption {
	return func(vmo *MountOptions) error {
		vmo.Nesting = false
		return nil
	}
}

// AsReadOnly specifies, if this mount is in a readonly state.
func AsReadOnly() MountOption {
	return func(vmo *MountOptions) error {
		vmo.ReadOnly = true
		return nil
	}
}
