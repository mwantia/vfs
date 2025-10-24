package backend

import (
	"context"

	"github.com/mwantia/vfs/data"
)

type VirtualObjectStorageBackend interface {
	VirtualBackend

	CreateObject(ctx context.Context, key string, fileType data.VirtualFileType, fileMode data.VirtualFileMode) (*data.VirtualFileStat, error)

	ReadObject(ctx context.Context, key string, offset int64, data []byte) (int, error)

	WriteObject(ctx context.Context, key string, offset int64, data []byte) (int, error)

	DeleteObject(ctx context.Context, key string, force bool) error

	ListObjects(ctx context.Context, key string) ([]*data.VirtualFileStat, error)

	HeadObject(ctx context.Context, key string) (*data.VirtualFileStat, error)

	TruncateObject(ctx context.Context, key string, size int64) error
}
