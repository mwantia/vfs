package sqlite

import (
	"context"
	"database/sql"
	"sync"

	"github.com/mwantia/vfs/backend"
	"github.com/tidwall/btree"
	_ "modernc.org/sqlite" // Pure Go SQLite driver
)

// SQLiteBackend provides a virtual filesystem backend using SQLite with three-layer architecture:
//
// Layer 1: In-memory B-tree for fast key → ID lookups (keys map)
// Layer 2: SQLite metadata table (vfs_metadata) for filesystem metadata
// Layer 3: SQLite data table (vfs_data) for file content with reference counting
//
// This architecture enables:
// - Fast path lookups via B-tree (O(log n))
// - Persistent metadata in SQLite
// - Content deduplication via reference counting
// - Transaction support for atomic operations
type SQLiteBackend struct {
	mu sync.RWMutex
	db *sql.DB

	// In-memory B-tree for fast key lookups
	keys *btree.Map[string, string]
}

// NewSQLiteBackend creates a new SQLite-backed virtual backend.
// The dbPath can be ":memory:" for an in-memory database or a file path.
func NewSQLiteBackend(dbPath string) (*SQLiteBackend, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	// Enable foreign keys for referential integrity
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, err
	}

	// Enable WAL mode for better concurrency
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, err
	}

	backend := &SQLiteBackend{
		db:   db,
		keys: btree.NewMap[string, string](0),
	}

	if err := backend.initSchema(); err != nil {
		db.Close()
		return nil, err
	}

	return backend, nil
}

// initSchema creates the database schema.
func (sb *SQLiteBackend) initSchema() error {
	schema := `
	-- Metadata storage
	CREATE TABLE IF NOT EXISTS vfs_metadata (
		id TEXT PRIMARY KEY,
		key TEXT NOT NULL UNIQUE,
		mode INTEGER NOT NULL,
		size INTEGER NOT NULL DEFAULT 0,
		uid INTEGER,
		gid INTEGER,
		modify_time INTEGER NOT NULL,
		access_time INTEGER NOT NULL,
		create_time INTEGER NOT NULL,
		content_type TEXT,
		etag TEXT,
		attributes TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_key ON vfs_metadata(key);

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
	`

	_, err := sb.db.Exec(schema)
	return err
}

// Returns the identifier name defined for this backend
func (*SQLiteBackend) GetName() string {
	return "sqlite"
}

// Open is part of the lifecycle behavious and gets called when opening this backend.
func (sb *SQLiteBackend) Open(ctx context.Context) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Verify database connection
	if err := sb.db.PingContext(ctx); err != nil {
		return err
	}

	// Load all keys into memory B-tree
	rows, err := sb.db.QueryContext(ctx, "SELECT key, id FROM vfs_metadata")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var key, id string
		if err := rows.Scan(&key, &id); err != nil {
			return err
		}
		sb.keys.Set(key, id)
	}

	return rows.Err()
}

// Close is part of the lifecycle behaviour and gets called when closing this backend.
func (sb *SQLiteBackend) Close(ctx context.Context) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	sb.keys.Clear()
	return sb.db.Close()
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (sb *SQLiteBackend) GetCapabilities() *backend.VirtualBackendCapabilities {
	return &backend.VirtualBackendCapabilities{
		Capabilities: []backend.VirtualBackendCapability{
			backend.CapabilityObjectStorage,
			backend.CapabilityMetadata,
		},
	}
}
