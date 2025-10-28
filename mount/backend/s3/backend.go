package s3

import (
	"context"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/backend"
)

type S3Backend struct {
	mu sync.RWMutex

	client     *minio.Client
	bucketName string
}

func NewS3Backend(endpoint, bucketName, accessKey, secretKey string, useSsl bool) (*S3Backend, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSsl,
	})
	if err != nil {
		return nil, err
	}

	return &S3Backend{
		client:     client,
		bucketName: bucketName,
	}, nil
}

// Returns the identifier name defined for this backend
func (*S3Backend) Name() string {
	return "s3"
}

// Open is part of the lifecycle behavious and gets called when opening this backend.
func (sb *S3Backend) Open(ctx context.Context) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	exists, err := sb.client.BucketExists(ctx, sb.bucketName)
	if err != nil {
		return err
	}

	if !exists {
		return data.ErrMountFailed
	}

	return nil
}

// Close is part of the lifecycle behaviour and gets called when closing this backend.
func (sb *S3Backend) Close(ctx context.Context) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	return nil
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (sb *S3Backend) GetCapabilities() *backend.VirtualBackendCapabilities {
	return &backend.VirtualBackendCapabilities{
		Capabilities: []backend.VirtualBackendCapability{
			backend.CapabilityObjectStorage,
		},
	}
}
