package backend

import (
	"context"

	"github.com/mwantia/vfs/data"
)

// VirtualMetadataBackend stores filesystem metadata (paths, sizes, timestamps, etc.)
// This is the "fast index" layer - optimized for queries and listing
type VirtualMetadataBackend interface {
	VirtualBackend

	CreateMeta(ctx context.Context, meta *data.VirtualFileMetadata) error

	ReadMeta(ctx context.Context, key string) (*data.VirtualFileMetadata, error)

	UpdateMeta(ctx context.Context, key string, update *data.VirtualFileMetadataUpdate) error

	DeleteMeta(ctx context.Context, key string) error

	ExistsMeta(ctx context.Context, key string) (bool, error)

	ReadAllMeta(ctx context.Context) ([]*data.VirtualFileMetadata, error)

	CreateAllMeta(ctx context.Context, metas []*data.VirtualFileMetadata) error
}
