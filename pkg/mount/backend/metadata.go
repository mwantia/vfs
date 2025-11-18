package backend

import (
	"github.com/mwantia/vfs/pkg/context"

	"github.com/mwantia/vfs/data"
)

// MetadataBackend stores filesystem metadata (paths, sizes, timestamps, etc.)
// This is the "fast index" layer - optimized for queries and listing
type MetadataBackend interface {
	Backend
	// CreateMeta
	CreateMeta(ctx context.TraversalContext, ns string, meta *data.Metadata) error
	// ReadMeta
	ReadMeta(ctx context.TraversalContext, ns string, key string) (*data.Metadata, error)
	// UpdateMeta
	UpdateMeta(ctx context.TraversalContext, ns string, key string, update *data.MetadataUpdate) error
	// DeleteMeta
	DeleteMeta(ctx context.TraversalContext, ns string, key string) error
	// ExistsMeta
	ExistsMeta(ctx context.TraversalContext, ns string, key string) (bool, error)
	// QueryMeta
	QueryMeta(ctx context.TraversalContext, ns string, query *Query) (*QueryResult, error)
}
