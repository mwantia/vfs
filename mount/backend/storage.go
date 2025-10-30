package backend

import (
	"context"

	"github.com/mwantia/vfs/data"
)

type ObjectStorageBackend interface {
	Backend

	CreateObject(ctx context.Context, namespace string, key string, mode data.FileMode) (*data.FileStat, error)

	ReadObject(ctx context.Context, namespace string, key string, offset int64, data []byte) (int, error)

	WriteObject(ctx context.Context, namespace string, key string, offset int64, data []byte) (int, error)

	DeleteObject(ctx context.Context, namespace string, key string, force bool) error

	ListObjects(ctx context.Context, namespace string, key string) ([]*data.FileStat, error)

	HeadObject(ctx context.Context, namespace string, key string) (*data.FileStat, error)

	TruncateObject(ctx context.Context, namespace string, key string, size int64) error
}
