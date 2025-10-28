package postgres

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mwantia/vfs/mount/backend"
	"github.com/tidwall/btree"
)

// PostgresBackend provides a virtual filesystem backend using PostgreSQL with three-layer architecture:
//
// Layer 1: In-memory B-tree for fast key → ID lookups (keys map)
// Layer 2: PostgreSQL metadata table (vfs_metadata) for filesystem metadata
// Layer 3: PostgreSQL data table (vfs_data) for file content with reference counting
//
// This architecture enables:
// - Fast path lookups via B-tree (O(log n))
// - Persistent metadata in PostgreSQL
// - Content deduplication via reference counting
// - Transaction support for atomic operations
// - PostgreSQL-specific features (JSONB, full-text search, etc.)
type PostgresBackend struct {
	mu   sync.RWMutex
	pool *pgxpool.Pool

	// In-memory B-tree for fast key lookups
	keys *btree.Map[string, string]
}

// NewPostgresBackend creates a new PostgreSQL-backed virtual backend.
// The connString should be a standard PostgreSQL connection string or URL.
// Example: "postgres://user:pass@localhost:5432/dbname"
func NewPostgresBackend(connString string) (*PostgresBackend, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// Disable prepared statement caching to avoid collisions in pooled connections
	// This is important for VFS backends that may be created/destroyed frequently in tests
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	backend := &PostgresBackend{
		pool: pool,
		keys: btree.NewMap[string, string](0),
	}

	if err := backend.initSchema(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return backend, nil
}

// initSchema creates the database schema.
func (pb *PostgresBackend) initSchema(ctx context.Context) error {
	// Split schema into individual statements to avoid prepared statement cache collisions
	statements := []string{
		`CREATE TABLE IF NOT EXISTS vfs_metadata (
			id TEXT PRIMARY KEY,
			key TEXT NOT NULL UNIQUE,
			mode BIGINT NOT NULL,
			size BIGINT NOT NULL DEFAULT 0,
			uid BIGINT,
			gid BIGINT,
			modify_time BIGINT NOT NULL,
			access_time BIGINT NOT NULL,
			create_time BIGINT NOT NULL,
			content_type TEXT,
			etag TEXT,
			attributes JSONB
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vfs_metadata_key ON vfs_metadata(key)`,
		`CREATE INDEX IF NOT EXISTS idx_vfs_metadata_prefix ON vfs_metadata(key text_pattern_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_vfs_metadata_content_type ON vfs_metadata(content_type)`,
		`CREATE INDEX IF NOT EXISTS idx_vfs_metadata_size ON vfs_metadata(size)`,
		`CREATE INDEX IF NOT EXISTS idx_vfs_metadata_modify_time ON vfs_metadata(modify_time)`,
		`CREATE INDEX IF NOT EXISTS idx_vfs_metadata_attributes ON vfs_metadata USING GIN(attributes)`,
		`CREATE TABLE IF NOT EXISTS vfs_data (
			id TEXT PRIMARY KEY,
			content BYTEA NOT NULL,
			size BIGINT NOT NULL CHECK(size >= 0),
			ref_count INTEGER NOT NULL DEFAULT 0,
			created_at BIGINT NOT NULL,
			last_accessed BIGINT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vfs_data_ref_count ON vfs_data(ref_count)`,
	}

	conn, err := pb.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Execute each statement individually
	for _, stmt := range statements {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute schema statement: %w", err)
		}
	}

	return nil
}

// Name returns the identifier name defined for this backend
func (*PostgresBackend) Name() string {
	return "postgres"
}

// Open is part of the lifecycle behavious and gets called when opening this backend.
func (pb *PostgresBackend) Open(ctx context.Context) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Verify database connection
	conn, err := pb.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Load all keys into memory B-tree
	rows, err := conn.Query(ctx, "SELECT key, id FROM vfs_metadata")
	if err != nil {
		return fmt.Errorf("failed to load keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key, id string
		if err := rows.Scan(&key, &id); err != nil {
			return fmt.Errorf("failed to scan key: %w", err)
		}
		pb.keys.Set(key, id)
	}

	return rows.Err()
}

// Close is part of the lifecycle behaviour and gets called when closing this backend.
func (pb *PostgresBackend) Close(ctx context.Context) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.keys.Clear()
	pb.pool.Close()
	return nil
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (pb *PostgresBackend) GetCapabilities() *backend.VirtualBackendCapabilities {
	return &backend.VirtualBackendCapabilities{
		Capabilities: []backend.VirtualBackendCapability{
			backend.CapabilityObjectStorage,
			backend.CapabilityMetadata,
		},
	}
}
