package extensions

import (
	"context"
	"time"
)

type CacheExtension interface {
	// GetCache
	GetCache(ctx context.Context, key string) ([]byte, error)

	// SetCache
	SetCache(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// DeleteCache
	DeleteCache(ctx context.Context, key string) error

	// ClearCache
	ClearCache(ctx context.Context) error

	// ExistsCache
	ExistsCache(ctx context.Context, key string) (bool, error)

	// GetBatchCache
	GetBatchCache(ctx context.Context, keys ...string) (map[string][]byte, error)

	// SetBatchCache
	SetBatchCache(ctx context.Context, items map[string][]byte, ttl time.Duration) error

	// DeleteBatchCache
	DeleteBatchCache(ctx context.Context, keys ...string) error
}
