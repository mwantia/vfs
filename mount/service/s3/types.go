package s3

import (
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/mwantia/vfs/mount/service"
)

var (
	DriverName = "s3"
	DriverType = service.ServiceTypeObjectStorage
)

// S3ObjectStorageDriver
type S3ObjectStorageDriver struct {
	mu sync.RWMutex

	client *minio.Client
	cfg    *S3BackendConfig
}

// S3ObjectStorageService
type S3ObjectStorageService struct {
	driver *S3ObjectStorageDriver
}

// S3BackendConfig
type S3BackendConfig struct {
	//
	Endpoint string `json:"endpoint"`

	//
	UseSSL bool `json:"usessl"`

	//
	BucketName string `json:"bucket_name"`

	//
	AccessKey string `json:"access_key"`

	//
	SecretKey string `json:"secret_key"`

	//
	Region string `json:"region"`
}
