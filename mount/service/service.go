package service

import (
	"context"

	"github.com/mwantia/vfs/data"
)

// Service
type Service interface {
	GetLifecycle() Lifecycle
}

// ObjectStorageService
type ObjectStorageService interface {
	//
	Service

	CreateObject(ctx context.Context, namespace, key string, mode data.FileMode) (*data.FileStat, error)

	ReadObject(ctx context.Context, namespace, key string, offset int64, data []byte) (int, error)

	WriteObject(ctx context.Context, namespace, key string, offset int64, data []byte) (int, error)

	DeleteObject(ctx context.Context, namespace, key string, force bool) error

	ListObjects(ctx context.Context, namespace, key string) ([]*data.FileStat, error)

	HeadObject(ctx context.Context, namespace, key string) (*data.FileStat, error)

	TruncateObject(ctx context.Context, namespace, key string, size int64) error
}

// MetadataService
type MetadataService interface {
	//
	Service

	CreateMeta(ctx context.Context, namespace string, meta *data.Metadata) error

	ReadMeta(ctx context.Context, namespace, key string) (*data.Metadata, error)

	UpdateMeta(ctx context.Context, namespace, key string, update *data.MetadataUpdate) error

	DeleteMeta(ctx context.Context, namespace, key string) error

	ExistsMeta(ctx context.Context, namespace, key string) (bool, error)

	QueryMeta(ctx context.Context, namespace string, query *Query) (*QueryPagination, error)
}
