package postgres

import (
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mwantia/vfs/mount/service"
)

var (
	DriverName = "postgres"
	DriverType = service.ServiceTypeMonolith
)

// PostgresMonolithDriver implements a monolith driver that provides both
// ObjectStorage and MetadataService backed by PostgreSQL
type PostgresMonolithDriver struct {
	mu sync.RWMutex

	// PostgreSQL connection pool
	pool *pgxpool.Pool

	// Configuration parsed from URI
	cfg *PostgresBackendConfig
}

// PostgresObjectStorageService implements ObjectStorageService
type PostgresObjectStorageService struct {
	driver *PostgresMonolithDriver
}

// PostgresMetadataService implements MetadataService
type PostgresMetadataService struct {
	driver *PostgresMonolithDriver
}

// PostgresMountExtensionService implements MountExtension
type PostgresMountExtensionService struct {
	driver *PostgresMonolithDriver
}

// PostgresBackendConfig holds configuration for the Postgres backend
type PostgresBackendConfig struct {
	ConnString string // PostgreSQL connection string
}
