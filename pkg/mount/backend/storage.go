package backend

import (
	"github.com/mwantia/vfs/pkg/context"

	"github.com/mwantia/vfs/data"
)

// ObjectStorageBackend
type ObjectStorageBackend interface {
	Backend
	// CreateObject
	CreateObject(ctx context.TraversalContext, ns string, key string, mode data.FileMode) (*data.FileStat, error)
	// ReadObject
	ReadObject(ctx context.TraversalContext, ns string, key string, offset int64, data []byte) (int, error)
	// WriteObject
	WriteObject(ctx context.TraversalContext, ns string, key string, offset int64, data []byte) (int, error)
	// DeleteObjects
	DeleteObject(ctx context.TraversalContext, ns string, key string, force bool) error
	// ListObjects
	ListObjects(ctx context.TraversalContext, ns string, key string) ([]*data.FileStat, error)
	// HeadObject
	HeadObject(ctx context.TraversalContext, ns string, key string) (*data.FileStat, error)
	// TruncateObject
	TruncateObject(ctx context.TraversalContext, ns string, key string, size int64) error
}
