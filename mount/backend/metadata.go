package backend

import (
	"context"

	"github.com/mwantia/vfs/data"
)

// MetadataBackend stores filesystem metadata (paths, sizes, timestamps, etc.)
// This is the "fast index" layer - optimized for queries and listing
type MetadataBackend interface {
	Backend

	CreateMeta(ctx context.Context, namespace string, meta *data.Metadata) error

	ReadMeta(ctx context.Context, namespace string, key string) (*data.Metadata, error)

	UpdateMeta(ctx context.Context, namespace string, key string, update *data.MetadataUpdate) error

	DeleteMeta(ctx context.Context, namespace string, key string) error

	ExistsMeta(ctx context.Context, namespace string, key string) (bool, error)

	QueryMeta(ctx context.Context, namespace string, query *MetadataQuery) (*MetadataQueryResult, error)
}
