package mount

import (
	"time"

	"github.com/mwantia/vfs/backend"
	"github.com/mwantia/vfs/extension/acl"
	"github.com/mwantia/vfs/extension/cache"
	"github.com/mwantia/vfs/extension/encrypt"
	"github.com/mwantia/vfs/extension/multipart"
	"github.com/mwantia/vfs/extension/rubbish"
	"github.com/mwantia/vfs/extension/snapshot"
	"github.com/mwantia/vfs/extension/versioning"
)

// VirtualMountOptions provides metadata about a mounted filesystem.
type VirtualMountOptions struct {
	acl        acl.VirtualAclBackend
	cache      cache.VirtualCacheBackend
	encrypt    encrypt.VirtualEncryptBackend
	metadata   backend.VirtualMetadataBackend
	multipart  multipart.VirtualMultipartBackend
	rubbish    rubbish.VirtualRubbishBackend
	snapshot   snapshot.VirtualSnapshotBackend
	versioning versioning.VirtualVersioningBackend

	Auto        bool      //
	MountTime   time.Time // When the mount was created.
	CacheReads  bool      // Cache file reads
	CacheWrites bool      // Buffer writes before upload
	ReadOnly    bool      // Whether the mount is read-only.
	Nesting     bool      // Whether the mount allows for nested mountpoints.
}

// VirtualMountOption configures mount behavior.
type VirtualMountOption func(*VirtualMountOptions) error

func NewDefaultOptions() *VirtualMountOptions {
	return &VirtualMountOptions{
		Auto:        true,
		MountTime:   time.Now(),
		CacheReads:  false,
		CacheWrites: false,
		ReadOnly:    false,
		Nesting:     true,
	}
}

func WithACL(acl acl.VirtualAclBackend) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.acl = acl
		return nil
	}
}

func WithCache(cache cache.VirtualCacheBackend) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.cache = cache
		return nil
	}
}

func WithEncrypt(encrypt encrypt.VirtualEncryptBackend) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.encrypt = encrypt
		return nil
	}
}

func WithMetadata(metadata backend.VirtualMetadataBackend) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.metadata = metadata
		return nil
	}
}

func WithMultipart(multipart multipart.VirtualMultipartBackend) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.multipart = multipart
		return nil
	}
}

func WithRubbish(rubbish rubbish.VirtualRubbishBackend) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.rubbish = rubbish
		return nil
	}
}

func WithSnapshot(snapshot snapshot.VirtualSnapshotBackend) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.snapshot = snapshot
		return nil
	}
}

func WithVersioning(versioning versioning.VirtualVersioningBackend) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.versioning = versioning
		return nil
	}
}

// WithAutoSync specifies, if autosync is enabled for this mount.
func DisableAuto() VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.Auto = false
		return nil
	}
}

// WithMountTime defines or overwrites the mounttime for this mount
func WithMountTime(mountTime time.Time) VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.MountTime = mountTime
		return nil
	}
}

// WithCacheReads specifies, if file reads should be cached for this mount.
func WithCacheReads() VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.CacheReads = true
		return nil
	}
}

// WithCacheWrites specifies, if write-operations will be buffered before upload.
func WithCacheWrites() VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.CacheWrites = true
		return nil
	}
}

// WithDenyNesting specifies, if nested mountpoints are allowed within this mount.
func DisableNesting() VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.Nesting = false
		return nil
	}
}

// AsReadOnly specifies, if this mount is in a readonly state.
func AsReadOnly() VirtualMountOption {
	return func(vmo *VirtualMountOptions) error {
		vmo.ReadOnly = true
		return nil
	}
}
