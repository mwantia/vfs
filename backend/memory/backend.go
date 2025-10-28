package memory

import (
	"context"
	"sync"

	"github.com/mwantia/vfs/backend"
	"github.com/mwantia/vfs/data"
	"github.com/tidwall/btree"
)

type MemoryBackend struct {
	mu sync.RWMutex

	keys     *btree.Map[string, string]
	metadata map[string]*data.VirtualFileMetadata

	datas       map[string][]byte
	directories map[string][]string
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		keys:        btree.NewMap[string, string](0),
		metadata:    make(map[string]*data.VirtualFileMetadata),
		datas:       make(map[string][]byte),
		directories: make(map[string][]string),
	}
}

// Returns the identifier name defined for this backend
func (*MemoryBackend) Name() string {
	return "memory"
}

// Open is part of the lifecycle behavious and gets called when opening this backend.
func (mb *MemoryBackend) Open(ctx context.Context) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// No initialization needed - backend is ready to use
	return nil
}

// Close is part of the lifecycle behaviour and gets called when closing this backend.
func (mb *MemoryBackend) Close(ctx context.Context) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	mb.keys.Clear()
	for k := range mb.metadata {
		delete(mb.metadata, k)
	}
	for k := range mb.datas {
		delete(mb.datas, k)
	}

	return nil
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (mb *MemoryBackend) GetCapabilities() *backend.VirtualBackendCapabilities {
	return &backend.VirtualBackendCapabilities{
		Capabilities: []backend.VirtualBackendCapability{
			backend.CapabilityObjectStorage,
			backend.CapabilityMetadata,
		},
	}
}
