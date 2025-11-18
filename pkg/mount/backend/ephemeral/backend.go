package ephemeral

import (
	"sync"

	"github.com/mwantia/vfs/pkg/context"
	"github.com/mwantia/vfs/pkg/mount/backend"

	"github.com/mwantia/vfs/data"
	"github.com/tidwall/btree"
)

type EphemeralBackend struct {
	backend.Backend
	backend.MetadataBackend

	mu sync.RWMutex

	keys     *btree.Map[string, string]
	dirs     map[string][]string
	metadata map[string]*data.Metadata
}

func NewEphemeralBackend() *EphemeralBackend {
	return &EphemeralBackend{
		keys: btree.NewMap[string, string](0),
	}
}

// Name returns the identifier name defined for this backend
func (*EphemeralBackend) Name() string {
	return "ephemeral"
}

// Open is part of the lifecycle behavious and gets called when opening this backend.
func (mb *EphemeralBackend) OpenBackend(ctx context.TraversalContext) error {
	return nil
}

// Close is part of the lifecycle behaviour and gets called when closing this backend.
func (mb *EphemeralBackend) CloseBackend(ctx context.TraversalContext) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	mb.keys.Clear()
	for k := range mb.metadata {
		delete(mb.metadata, k)
	}

	return nil
}

// IsBusy determines and returns if it's safe to close this backend.
func (mb *EphemeralBackend) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the file is busy
	if !mb.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	mb.mu.Unlock()
	return false
}

// Health returns the basic and fastest result to check the lifecycle and availablility of this backend.
func (mb *EphemeralBackend) Health() bool {
	// It should be safe to always return true
	return true
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (mb *EphemeralBackend) GetCapabilities() *backend.Capabilities {
	return &backend.Capabilities{
		Capabilities: []backend.Capability{
			{
				Type: backend.CapabilityMetadata,
			},
		},
	}
}
