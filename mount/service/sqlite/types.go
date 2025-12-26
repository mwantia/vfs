package sqlite

import (
	"database/sql"
	"sync"

	"github.com/mwantia/vfs/mount/service"
	"github.com/tidwall/btree"
)

var (
	DriverName = "sqlite"
	DriverType = service.ServiceTypeMonolith
)

type SqliteMonolithDriver struct {
	mu sync.RWMutex

	// Database connection
	db *sql.DB

	// In-memory B-tree for fast key → ID lookups
	// Keys are namespaced: "namespace:key" → metadata ID
	keys *btree.Map[string, string]

	// Configuration parsed from URI
	cfg *SqliteBackendConfig
}

type SqliteMetadataService struct {
	driver *SqliteMonolithDriver
}

type SqliteObjectStorageService struct {
	driver *SqliteMonolithDriver
}

type SqliteMountExtensionService struct {
	driver *SqliteMonolithDriver
}

type SqliteBackendConfig struct {
	Path        string // Database file path or ":memory:"
	WALMode     bool   // Enable Write-Ahead Logging
	ForeignKeys bool   // Enable foreign key constraints
}
