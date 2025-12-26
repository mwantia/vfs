package sqlite

import (
	"context"
	"database/sql"

	"github.com/mwantia/vfs/mount/service"
	"github.com/tidwall/btree"
	_ "modernc.org/sqlite" // Pure Go SQLite driver
)

const sqliteSchema = `
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
		spec TEXT NOT NULL
	);
`

func NewSqliteMonolithDriver(uri *service.Uri) (*SqliteMonolithDriver, error) {
	cfg := parseSqliteBackendConfig(uri)

	return &SqliteMonolithDriver{
		cfg:  cfg,
		keys: btree.NewMap[string, string](0),
	}, nil
}

// Name
func (*SqliteMonolithDriver) Name() string {
	return DriverName
}

// Health returns the health status of the driver by verifying database connectivity
func (d *SqliteMonolithDriver) Health() (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.db == nil {
		return false, nil
	}

	// Execute a simple query to verify connection is alive
	if err := d.db.Ping(); err != nil {
		return false, err
	}

	return true, nil
}

// IsBusy checks if the driver is currently busy with operations
func (d *SqliteMonolithDriver) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the driver is busy
	if !d.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	d.mu.Unlock()
	return false
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (*SqliteMonolithDriver) GetCapabilities() *service.DriverCapabilities {
	return &service.DriverCapabilities{
		Type: DriverType,
		Extensions: []service.ServiceExtension{
			service.ServiceExtensionMount,
		},
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
				MaxObjectSize: 5242880,
				MaxPathLength: 255,
				CaseSensitive: true,
			},
		},
	}
}

// OpenDriver initializes the database connection and loads existing keys into memory
func (d *SqliteMonolithDriver) OpenDriver(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Open database connection
	db, err := sql.Open("sqlite", d.cfg.Path)
	if err != nil {
		return err
	}

	// Enable foreign keys for referential integrity
	if d.cfg.ForeignKeys {
		if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
			db.Close()
			return err
		}
	}

	// Enable WAL mode for better concurrency
	if d.cfg.WALMode {
		if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
			db.Close()
			return err
		}
	}

	// Create schema if it doesn't exist
	if _, err := db.ExecContext(ctx, sqliteSchema); err != nil {
		db.Close()
		return err
	}

	// Load all existing keys into B-tree for fast lookups
	rows, err := db.QueryContext(ctx, "SELECT namespace, key, id FROM vfs_metadata")
	if err != nil {
		db.Close()
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var namespace, key, id string
		if err := rows.Scan(&namespace, &key, &id); err != nil {
			db.Close()
			return err
		}

		// Use NamedKey helper to create namespaced key
		namedKey := service.NamedKey(namespace, key, ":")
		d.keys.Set(namedKey, id)
	}

	if err := rows.Err(); err != nil {
		db.Close()
		return err
	}

	d.db = db
	return nil
}

// CloseDriver cleans up database connection and clears in-memory data
func (d *SqliteMonolithDriver) CloseDriver(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Clear the B-tree index
	d.keys.Clear()

	// Close database connection if it exists
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			return err
		}
		d.db = nil
	}

	return nil
}

// GetObjectStorageService
func (d *SqliteMonolithDriver) GetObjectStorageService() service.ObjectStorageService {
	return &SqliteObjectStorageService{
		driver: d,
	}
}

// GetMetadataService
func (d *SqliteMonolithDriver) GetMetadataService() service.MetadataService {
	return &SqliteMetadataService{
		driver: d,
	}
}

// GetExtensionService
func (d *SqliteMonolithDriver) GetExtensionService(ext service.ServiceExtension) service.Service {
	if ext == service.ServiceExtensionMount {
		return &SqliteMountExtensionService{
			driver: d,
		}
	}

	return nil
}

// parseSqliteBackendConfig parses URI into SQLite configuration
func parseSqliteBackendConfig(uri *service.Uri) *SqliteBackendConfig {
	cfg := &SqliteBackendConfig{
		Path:        ":memory:", // Default to in-memory database
		WALMode:     true,       // Enable WAL mode by default for better concurrency
		ForeignKeys: true,       // Enable foreign keys by default
	}

	// Parse path from URI
	// sqlite:// → :memory:
	// sqlite:///path/to/db → /path/to/db
	// sqlite://path/to/db → path/to/db
	if uri.Host != "" {
		// Host is set, combine with path
		cfg.Path = uri.Host + uri.Path
	} else if uri.Path != "" && uri.Path != "/" {
		// Only path is set
		cfg.Path = uri.Path
	}

	// Parse query parameters for pragma options
	if uri.Query != nil {
		// Future: parse pragma options from query string
		// Example: ?wal=false, ?foreign_keys=false
		if wal, ok := uri.Query["wal"]; ok && len(wal) > 0 && wal == "false" {
			cfg.WALMode = false
		}
		if fk, ok := uri.Query["foreign_keys"]; ok && len(fk) > 0 && fk == "false" {
			cfg.ForeignKeys = false
		}
	}

	return cfg
}
