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

var (
	Shema = `
	-- Metadata storage
	CREATE TABLE IF NOT EXISTS vfs_metadata (
		id TEXT PRIMARY KEY,
		namespace TEXT NOT NULL DEFAULT '',
		key TEXT NOT NULL,
		mode INTEGER NOT NULL,
		size INTEGER NOT NULL DEFAULT 0,
		uid INTEGER,
		gid INTEGER,
		modify_time INTEGER NOT NULL,
		access_time INTEGER NOT NULL,
		create_time INTEGER NOT NULL,
		content_type TEXT,
		etag TEXT,
		attributes TEXT,
		UNIQUE(namespace, key)
	);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_namespace_key ON vfs_metadata(namespace, key);

	-- Content storage with reference counting
	CREATE TABLE IF NOT EXISTS vfs_data (
		id TEXT PRIMARY KEY,
		content BLOB NOT NULL,
		size INTEGER NOT NULL CHECK(size >= 0),
		ref_count INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL,
		last_accessed INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_vfs_data_ref_count ON vfs_data(ref_count);

	-- Mount specifications (virtual /etc/fstab)
	CREATE TABLE IF NOT EXISTS vfs_mounts (
		path TEXT PRIMARY KEY,
		object_storage TEXT,
		metadata TEXT,
		specs TEXT NOT NULL
	);`
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
