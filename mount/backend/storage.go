package backend

import (
	"context"

	"github.com/mwantia/vfs/data"
)

type ObjectStorageBackend interface {
	Backend

	Namespace() string

	CreateObject(ctx context.Context, key string, mode data.FileMode) (*data.FileStat, error)

	ReadObject(ctx context.Context, key string, offset int64, data []byte) (int, error)

	WriteObject(ctx context.Context, key string, offset int64, data []byte) (int, error)

	DeleteObject(ctx context.Context, key string, force bool) error

	ListObjects(ctx context.Context, key string) ([]*data.FileStat, error)

	HeadObject(ctx context.Context, key string) (*data.FileStat, error)

	TruncateObject(ctx context.Context, key string, size int64) error
}
