package s3

import (
	"context"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mwantia/vfs/mount/service"
)

// NewS3ObjectStorageDriver
func NewS3ObjectStorageDriver(uri *service.Uri, useSsl bool) (*S3ObjectStorageDriver, error) {
	cfg := parseS3BackendConfig(uri, useSsl)
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: useSsl,
	})
	if err != nil {
		return nil, err
	}

	return &S3ObjectStorageDriver{
		cfg:    cfg,
		client: client,
	}, nil
}

// Name
func (*S3ObjectStorageDriver) Name() string {
	return DriverName
}

// Health returns the health status of the driver by checking S3 bucket accessibility
func (d *S3ObjectStorageDriver) Health() (bool, error) {
	// Check if bucket exists and is accessible
	exists, err := d.client.BucketExists(context.Background(), d.cfg.BucketName)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, fmt.Errorf("bucket %s does not exist", d.cfg.BucketName)
	}
	return true, nil
}

// IsBusy checks if the driver is currently busy with operations
func (d *S3ObjectStorageDriver) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the driver is busy
	if !d.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	d.mu.Unlock()
	return false
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (*S3ObjectStorageDriver) GetCapabilities() *service.DriverCapabilities {
	return &service.DriverCapabilities{
		Type: service.ServiceTypeObjectStorage,
		ObjectStorage: service.ObjectStorageCapabilities{
			Operations: []service.ObjectStorageOperation{
				service.ObjectStorageOperationCreate,
				service.ObjectStorageOperationRead,
				service.ObjectStorageOperationWrite,
				service.ObjectStorageOperationDelete,
				service.ObjectStorageOperationList,
				service.ObjectStorageOperationStream,
			},
			AccessModes: []service.ObjectStorageAccessMode{
				service.ObjectStorageAccessReadWrite,
			},
			Constraints: service.ObjectStorageConstraints{
				// S3 supports objects from 0 bytes to 5TB, but we set a practical limit of 5GB
				// for typical VFS use cases. Adjust as needed for your requirements.
				MaxObjectSize: 5368709120,
				// TODO :: Anything to change here for S3?
				MaxPathLength: 255,
				CaseSensitive: true,
			},
		},
	}
}

// OpenDriver validates that the S3 bucket exists and is accessible
func (d *S3ObjectStorageDriver) OpenDriver(ctx context.Context) error {
	// Verify bucket exists
	exists, err := d.client.BucketExists(ctx, d.cfg.BucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("bucket %s does not exist", d.cfg.BucketName)
	}
	return nil
}

// CloseDriver
func (*S3ObjectStorageDriver) CloseDriver(ctx context.Context) error {
	return nil
}

// GetObjectStorageService
func (d *S3ObjectStorageDriver) GetObjectStorageService() service.ObjectStorageService {
	return &S3ObjectStorageService{
		driver: d,
	}
}

// GetMetadataService
func (*S3ObjectStorageDriver) GetMetadataService() service.MetadataService {
	return nil
}

// GetExtensionService
func (*S3ObjectStorageDriver) GetExtensionService(ext service.ServiceExtension) service.Service {
	return nil
}

// parseS3BackendConfig extracts S3BackendConfig from Uri
// URI format: s3[s]://host:port/bucketname?accesskey=xxx&secretkey=xxx&region=us-east-1
// Alternative: s3[s]://accesskey:secretkey@host:port/bucketname?region=us-east-1
func parseS3BackendConfig(uri *service.Uri, useSsl bool) *S3BackendConfig {
	cfg := &S3BackendConfig{
		Endpoint:   "localhost:9000",
		BucketName: "",
		AccessKey:  "",
		SecretKey:  "",
		Region:     "us-east-1",
		UseSSL:     useSsl,
	}

	// Parse endpoint (host:port)
	if uri.Host != "" {
		if uri.Port > 0 {
			cfg.Endpoint = fmt.Sprintf("%s:%d", uri.Host, uri.Port)
		} else {
			cfg.Endpoint = uri.Host
		}
	}

	// Parse bucket name from path (strip leading "/")
	if uri.Path != "" {
		cfg.BucketName = strings.TrimPrefix(uri.Path, "/")
	}

	// Parse credentials - priority: Username/Password > Query params
	if uri.Username != "" {
		cfg.AccessKey = uri.Username
	} else if accessKey, ok := uri.Query["accesskey"]; ok {
		cfg.AccessKey = accessKey
	}

	if uri.Password != "" {
		cfg.SecretKey = uri.Password
	} else if secretKey, ok := uri.Query["secretkey"]; ok {
		cfg.SecretKey = secretKey
	}

	// Parse optional region
	if region, ok := uri.Query["region"]; ok {
		cfg.Region = region
	}

	return cfg
}
