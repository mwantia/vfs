package extensions

import (
	"time"

	"github.com/mwantia/vfs/context"
)

type CacheExtension interface {
	// GetCache
	GetCache(traversal context.TraversalContext, key string) ([]byte, error)

	// SetCache
	SetCache(traversal context.TraversalContext, key string, value []byte, ttl time.Duration) error

	// DeleteCache
	DeleteCache(traversal context.TraversalContext, key string) error

	// ClearCache
	ClearCache(traversal context.TraversalContext) error

	// ExistsCache
	ExistsCache(traversal context.TraversalContext, key string) (bool, error)

	// GetBatchCache
	GetBatchCache(traversal context.TraversalContext, keys ...string) (map[string][]byte, error)

	// SetBatchCache
	SetBatchCache(traversal context.TraversalContext, items map[string][]byte, ttl time.Duration) error

	// DeleteBatchCache
	DeleteBatchCache(traversal context.TraversalContext, keys ...string) error
}
