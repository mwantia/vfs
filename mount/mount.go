package mount

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mwantia/vfs/backend"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/extension/acl"
	"github.com/mwantia/vfs/extension/cache"
	"github.com/mwantia/vfs/extension/encrypt"
	"github.com/mwantia/vfs/extension/multipart"
	"github.com/mwantia/vfs/extension/rubbish"
	"github.com/mwantia/vfs/extension/snapshot"
	"github.com/mwantia/vfs/extension/versioning"
)

// MountInfo holds configuration and metadata towards the specified mount
type Mount struct {
	mu        sync.RWMutex
	streamers map[string]*MountStreamer

	Path      string
	Options   *MountOptions
	MountTime time.Time // When the mount was created.

	ObjectStorage backend.VirtualObjectStorageBackend
	Metadata      backend.VirtualMetadataBackend
	IsDualMount   bool

	ACL        acl.VirtualAclBackend
	Cache      cache.VirtualCacheBackend
	Encrypt    encrypt.VirtualEncryptBackend
	Multipart  multipart.VirtualMultipartBackend
	Rubbish    rubbish.VirtualRubbishBackend
	Snapshot   snapshot.VirtualSnapshotBackend
	Versioning versioning.VirtualVersioningBackend
}

func NewMountInfo(path string, primary backend.VirtualObjectStorageBackend, opts ...MountOption) (*Mount, error) {
	options := newDefaultMountOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	mnt := &Mount{
		streamers: make(map[string]*MountStreamer),

		Path:          path,
		Options:       options,
		MountTime:     time.Now(),
		ObjectStorage: primary,
	}

	caps := primary.GetCapabilities()

	// Perform capability check for extension ACL
	if ext, exists := options.Backends[backend.CapabilityACL]; exists {
		// Type validation for interface
		acl, ok := ext.(acl.VirtualAclBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse extension '%s' for ACL backend", ext.Name())
		}
		mnt.ACL = acl
	} else if options.Auto && caps.Contains(backend.CapabilityACL) {
		acl, ok := primary.(acl.VirtualAclBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse '%s' for ACL backend", ext.Name())
		}
		mnt.ACL = acl
	}
	// Perform capability check for extension Cache
	if ext, exists := options.Backends[backend.CapabilityCache]; exists {
		// Type validation for interface
		cache, ok := ext.(cache.VirtualCacheBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse extension '%s' for Cache backend", ext.Name())
		}
		mnt.Cache = cache
	} else if options.Auto && caps.Contains(backend.CapabilityCache) {
		cache, ok := primary.(cache.VirtualCacheBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse '%s' for Cache backend", ext.Name())
		}
		mnt.Cache = cache
	}
	// Perform capability check for extension Encrypt
	if ext, exists := options.Backends[backend.CapabilityEncrypt]; exists {
		// Type validation for interface
		encrypt, ok := ext.(encrypt.VirtualEncryptBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse extension '%s' for Encrypt backend", ext.Name())
		}
		mnt.Encrypt = encrypt
	} else if options.Auto && caps.Contains(backend.CapabilityEncrypt) {
		encrypt, ok := primary.(encrypt.VirtualEncryptBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse '%s' for Encrypt backend", ext.Name())
		}
		mnt.Encrypt = encrypt
	}
	// Perform capability check for extension Metadata
	if ext, exists := options.Backends[backend.CapabilityMetadata]; exists {
		// Type validation for interface
		metadata, ok := ext.(backend.VirtualMetadataBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse extension '%s' for Metadata backend", ext.Name())
		}
		mnt.Metadata = metadata
	} else if options.Auto && caps.Contains(backend.CapabilityMetadata) {
		metadata, ok := primary.(backend.VirtualMetadataBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse '%s' for Metadata backend", primary.Name())
		}
		mnt.Metadata = metadata
		// Set IsDualMount to true, since most operations need to be simplified
		mnt.IsDualMount = true
	}
	// Perform capability check for extension Multipart
	if ext, exists := options.Backends[backend.CapabilityMultipart]; exists {
		// Type validation for interface
		multipart, ok := ext.(multipart.VirtualMultipartBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse extension '%s' for Multipart backend", ext.Name())
		}
		mnt.Multipart = multipart
	} else if options.Auto && caps.Contains(backend.CapabilityMultipart) {
		multipart, ok := primary.(multipart.VirtualMultipartBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse '%s' for Multipart backend", ext.Name())
		}
		mnt.Multipart = multipart
	}
	// Perform capability check for extension Rubbish
	if ext, exists := options.Backends[backend.CapabilityRubbish]; exists {
		// Type validation for interface
		rubbish, ok := ext.(rubbish.VirtualRubbishBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse extension '%s' for Rubbish backend", ext.Name())
		}
		mnt.Rubbish = rubbish
	} else if options.Auto && caps.Contains(backend.CapabilityRubbish) {
		rubbish, ok := primary.(rubbish.VirtualRubbishBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse '%s' for Rubbish backend", ext.Name())
		}
		mnt.Rubbish = rubbish
	}
	// Perform capability check for extension Snapshot
	if ext, exists := options.Backends[backend.CapabilitySnapshot]; exists {
		// Type validation for interface
		snapshot, ok := ext.(snapshot.VirtualSnapshotBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse extension '%s' for Snapshot backend", ext.Name())
		}
		mnt.Snapshot = snapshot
	} else if options.Auto && caps.Contains(backend.CapabilitySnapshot) {
		snapshot, ok := primary.(snapshot.VirtualSnapshotBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse '%s' for Snapshot backend", ext.Name())
		}
		mnt.Snapshot = snapshot
	}
	// Perform capability check for extension Versioning
	if ext, exists := options.Backends[backend.CapabilityVersioning]; exists {
		// Type validation for interface
		versioning, ok := ext.(versioning.VirtualVersioningBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse extension '%s' for Versioning backend", ext.Name())
		}
		mnt.Versioning = versioning
	} else if options.Auto && caps.Contains(backend.CapabilityVersioning) {
		versioning, ok := primary.(versioning.VirtualVersioningBackend)
		if !ok {
			return nil, fmt.Errorf("failed to parse '%s' for Versioning backend", ext.Name())
		}
		mnt.Versioning = versioning
	}

	if !mnt.IsDualMount {
		primaryAsMetadata, ok := primary.(backend.VirtualMetadataBackend)
		// Additional fallback check and validation
		// Occurs if primary and metadata are defined manually
		if ok && primaryAsMetadata == mnt.Metadata {
			mnt.IsDualMount = true
		}
	}

	return mnt, nil
}

func (m *Mount) Mount(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	errs := data.Errors{}
	// Open all backends/extensions set to this mount
	for _, vb := range m.getUniqueBackends() {
		if err := vb.Open(ctx); err != nil {
			errs.Add(err)
		}
	}

	return errs.Errors()
}

func (m *Mount) Unmount(ctx context.Context, force bool) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !force {
		// Initial check, to see if we have any busy streamers
		for _, streamer := range m.streamers {
			if streamer.IsBusy() {
				// Fail, since we shouldn't unmount busy backends
				return data.ErrBusy
			}
		}
	}

	for _, streamer := range m.streamers {
		if streamer.IsBusy() && !force {
			return data.ErrBusy
		} else {
			// Close the streamer
			if err := streamer.Close(); err != nil {
				return err
			}
		}
	}

	errs := data.Errors{}
	// Open all backends/extensions set to this mount
	for _, vb := range m.getUniqueBackends() {
		if err := vb.Close(ctx); err != nil {
			errs.Add(err)
		}
	}

	return errs.Errors()
}

func (m *Mount) GetStreamer(path string) (Streamer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	streamer, exists := m.streamers[path]
	if !exists {
		return nil, false
	}

	return streamer, true
}

func (m *Mount) OpenStreamer(ctx context.Context, path string, offset int64, flags data.VirtualAccessMode) Streamer {
	m.mu.Lock()
	defer m.mu.Unlock()

	streamer := newMountStreamer(ctx, m, path, offset, flags)
	m.streamers[path] = streamer

	return streamer
}

func (m *Mount) CloseStreamer(ctx context.Context, path string, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	streamer, exists := m.streamers[path]
	if !exists {
		return data.ErrNotExist
	}

	if streamer.IsBusy() && !force {
		return data.ErrBusy
	}

	return streamer.Close()
}

// getUniqueBackends returns a list of unique backends without duplicates
func (m *Mount) getUniqueBackends() []backend.VirtualBackend {
	// Create list of all available backends
	backends := []backend.VirtualBackend{
		m.ObjectStorage,
		m.ACL,
		m.Cache,
		m.Encrypt,
		m.Metadata,
		m.Multipart,
		m.Rubbish,
		m.Snapshot,
		m.Versioning,
	}
	// Use map to track unique backend pointers
	seen := make(map[backend.VirtualBackend]struct{})
	// Helper to add backend if not nil and not already seen
	addBackend := func(b backend.VirtualBackend) {
		if b != nil {
			seen[b] = struct{}{}
		}
	}
	// Collect all backends
	for _, backend := range backends {
		addBackend(backend)
	}
	// Convert map keys to slice
	uniques := make([]backend.VirtualBackend, 0, len(seen))
	for b := range seen {
		uniques = append(uniques, b)
	}

	return uniques
}
