package postgres

import (
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mwantia/vfs/mount/service"
	"github.com/tidwall/btree"
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

	// In-memory B-tree for fast key → ID lookups
	// Keys are namespaced: "namespace:key" → metadata ID
	keys *btree.Map[string, string]

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

// PostgresBackendConfig holds configuration for the Postgres backend
type PostgresBackendConfig struct {
	ConnString string // PostgreSQL connection string
}
