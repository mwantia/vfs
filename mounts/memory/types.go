package memory

import (
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/mwantia/vfs/data"
	"github.com/tidwall/btree"
)

// MetaMemoryMount is a thread-safe in-memory filesystem implementation with three-layer architecture:
//
// Layer 1 (paths):  B-tree mapping path → inode ID for fast ordered lookups
// Layer 2 (inodes): Map of inode ID → metadata (VirtualInode) for filesystem operations
// Layer 3 (datas):  Map of inode ID → content ([]byte) for content-addressable storage
//
// This architecture enables:
// - Fast path lookups and prefix scans (O(log n))
// - Content deduplication (multiple paths → same data)
// - Hard link support (multiple paths → same inode)
// - Efficient List operations via B-tree range scans
//
// Design rationale:
// This structure serves as a proof-of-concept for production storage backends (S3, Redis, etc.)
// where the metadata layer can cache/optimize lookups while the data layer handles actual storage.
type MetaMemoryMount struct {
	mu sync.RWMutex

	// Layer 1: Path index - B-tree for ordered path → inode ID mapping
	// Enables O(log n) lookups and efficient prefix scans for directory listings
	paths *btree.Map[string, string]

	// Layer 2: Inode metadata - Fast lookup of inode information
	// In production backends, this would be a cache of frequently accessed metadata
	inodes map[string]*data.VirtualInode

	// Layer 3: Content storage - Content-addressable data store
	// Multiple inodes can reference the same data ID (deduplication/hard links)
	// In production, this would be S3/Redis/etc with content-based addressing
	datas map[string][]byte

	// Atomic counter for generating unique inode IDs
	nextID int64
}

// NewMetaMemoryMount creates a new in-memory filesystem with an empty root directory.
func NewMetaMemoryMount() *MetaMemoryMount {
	return &MetaMemoryMount{
		paths:  btree.NewMap[string, string](0), // degree 0 = auto-optimize
		inodes: make(map[string]*data.VirtualInode),
		datas:  make(map[string][]byte),
		// Start at 1 (0 can be reserved for invalid/uninitialized)
		nextID: 1,
	}
}

// generateInodeID atomically generates a thread-safe unique inode ID.
func (m *MetaMemoryMount) generateInodeID() string {
	id := atomic.AddInt64(&m.nextID, 1)
	return strconv.FormatInt(id, 10)
}
