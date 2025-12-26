package vfs_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/mount"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestServiceFactory func(ctx context.Context, vfs vfs.VirtualFileSystem) error

var Factories = map[string]TestServiceFactory{
	"ephemeral-object-storage-only": func(ctx context.Context, vfs vfs.VirtualFileSystem) error {
		return vfs.Mount(ctx, "/", mount.WithObjectStorage("ephemeral://"))
	},
	"ephemeral-object-storage-metadata": func(ctx context.Context, vfs vfs.VirtualFileSystem) error {
		return vfs.Mount(ctx, "/", mount.WithObjectStorage("ephemeral://"), mount.WithMetadata("ephemeral://"))
	},
	"sqlite-memory-object-storage-only": func(ctx context.Context, vfs vfs.VirtualFileSystem) error {
		return vfs.Mount(ctx, "/", mount.WithObjectStorage("sqlite://"))
	},
	"sqlite-memory-object-storage-metadata": func(ctx context.Context, vfs vfs.VirtualFileSystem) error {
		return vfs.Mount(ctx, "/", mount.WithObjectStorage("sqlite://"), mount.WithMetadata("sqlite://"))
	},
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start Consul container
	consulContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "hashicorp/consul:latest",
			ExposedPorts: []string{"8500/tcp"},
			WaitingFor:   wait.ForHTTP("/v1/status/leader").WithPort("8500/tcp"),
		},
		Started: true,
	})
	if err != nil {
		fmt.Printf("Failed to start Consul container: %v\n", err)
		os.Exit(1)
	}
	defer consulContainer.Terminate(ctx)

	// Get Consul host and port
	consulHost, err := consulContainer.Host(ctx)
	if err != nil {
		fmt.Printf("Failed to get Consul host: %v\n", err)
		os.Exit(1)
	}

	consulPort, err := consulContainer.MappedPort(ctx, "8500")
	if err != nil {
		fmt.Printf("Failed to get Consul port: %v\n", err)
		os.Exit(1)
	}

	// Build Consul URI
	consulURI := fmt.Sprintf("consul://%s:%s", consulHost, consulPort.Port())
	Factories["consul-object-storage-only"] = func(ctx context.Context, vfs vfs.VirtualFileSystem) error {
		return vfs.Mount(ctx, "/", mount.WithObjectStorage(consulURI))
	}
	Factories["consul-object-storage-ephemeral-metadata"] = func(ctx context.Context, vfs vfs.VirtualFileSystem) error {
		return vfs.Mount(ctx, "/", mount.WithObjectStorage(consulURI), mount.WithMetadata("ephemeral://"))
	}

	// Start MinIO container for S3 tests
	minioContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "minio/minio:latest",
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     "minioadmin",
				"MINIO_ROOT_PASSWORD": "minioadmin",
			},
			Cmd:        []string{"server", "/data"},
			WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp"),
		},
		Started: true,
	})
	if err != nil {
		fmt.Printf("Failed to start MinIO container: %v\n", err)
		os.Exit(1)
	}
	defer minioContainer.Terminate(ctx)

	// Get MinIO host and port
	minioHost, err := minioContainer.Host(ctx)
	if err != nil {
		fmt.Printf("Failed to get MinIO host: %v\n", err)
		os.Exit(1)
	}

	minioPort, err := minioContainer.MappedPort(ctx, "9000")
	if err != nil {
		fmt.Printf("Failed to get MinIO port: %v\n", err)
		os.Exit(1)
	}

	// Create MinIO client to create bucket
	minioClient, err := minio.New(fmt.Sprintf("%s:%s", minioHost, minioPort.Port()), &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		fmt.Printf("Failed to create MinIO client: %v\n", err)
		os.Exit(1)
	}

	// Create test bucket
	bucketName := "test-bucket"
	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		// Check if bucket already exists
		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if errBucketExists != nil || !exists {
			fmt.Printf("Failed to create bucket: %v\n", err)
			os.Exit(1)
		}
	}

	// Build S3 URI
	s3URI := fmt.Sprintf("s3://%s:%s/%s?accesskey=minioadmin&secretkey=minioadmin", minioHost, minioPort.Port(), bucketName)
	Factories["s3-object-storage-only"] = func(ctx context.Context, vfs vfs.VirtualFileSystem) error {
		return vfs.Mount(ctx, "/", mount.WithObjectStorage(s3URI))
	}
	Factories["s3-object-storage-ephemeral-metadata"] = func(ctx context.Context, vfs vfs.VirtualFileSystem) error {
		return vfs.Mount(ctx, "/", mount.WithObjectStorage(s3URI), mount.WithMetadata("ephemeral://"))
	}

	// Run tests
	code := m.Run()

	os.Exit(code)
}
