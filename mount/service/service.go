package service

import (
	traversal "github.com/mwantia/vfs/context"
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

	CreateObject(traversal traversal.TraversalContext, namespace, key string, mode data.FileMode) (*data.FileStat, error)

	ReadObject(traversal traversal.TraversalContext, namespace, key string, offset int64, data []byte) (int, error)

	WriteObject(traversal traversal.TraversalContext, namespace, key string, offset int64, data []byte) (int, error)

	DeleteObject(traversal traversal.TraversalContext, namespace, key string, force bool) error

	ListObjects(traversal traversal.TraversalContext, namespace, key string) ([]*data.FileStat, error)

	HeadObject(traversal traversal.TraversalContext, namespace, key string) (*data.FileStat, error)

	TruncateObject(traversal traversal.TraversalContext, namespace, key string, size int64) error
}

// MetadataService
type MetadataService interface {
	//
	Service

	CreateMeta(traversal traversal.TraversalContext, namespace string, meta *data.Metadata) error

	ReadMeta(traversal traversal.TraversalContext, namespace, key string) (*data.Metadata, error)

	UpdateMeta(traversal traversal.TraversalContext, namespace, key string, update *data.MetadataUpdate) error

	DeleteMeta(traversal traversal.TraversalContext, namespace, key string) error

	ExistsMeta(traversal traversal.TraversalContext, namespace, key string) (bool, error)

	QueryMeta(traversal traversal.TraversalContext, namespace string, query *Query) (*QueryPagination, error)
}
