package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mwantia/vfs/mount/service"
	"github.com/tidwall/btree"
)

const postgresSchema = `
	-- Metadata storage
	CREATE TABLE IF NOT EXISTS vfs_metadata (
		id TEXT PRIMARY KEY,
		namespace TEXT NOT NULL DEFAULT '',
		key TEXT NOT NULL,
		mode BIGINT NOT NULL,
		size BIGINT NOT NULL DEFAULT 0,
		uid BIGINT,
		gid BIGINT,
		modify_time BIGINT NOT NULL,
		access_time BIGINT NOT NULL,
		create_time BIGINT NOT NULL,
		content_type TEXT,
		etag TEXT,
		attributes JSONB,
		UNIQUE(namespace, key)
	);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_namespace_key ON vfs_metadata(namespace, key);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_key ON vfs_metadata(key);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_prefix ON vfs_metadata(key text_pattern_ops);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_content_type ON vfs_metadata(content_type);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_size ON vfs_metadata(size);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_modify_time ON vfs_metadata(modify_time);
	CREATE INDEX IF NOT EXISTS idx_vfs_metadata_attributes ON vfs_metadata USING GIN(attributes);

	-- Content storage with reference counting
	CREATE TABLE IF NOT EXISTS vfs_data (
		id TEXT PRIMARY KEY,
		content BYTEA NOT NULL,
		size BIGINT NOT NULL CHECK(size >= 0),
		ref_count INTEGER NOT NULL DEFAULT 0,
		created_at BIGINT NOT NULL,
		last_accessed BIGINT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_vfs_data_ref_count ON vfs_data(ref_count);
`

// NewPostgresMonolithDriver creates a new Postgres monolith driver
func NewPostgresMonolithDriver(uri *service.Uri) (*PostgresMonolithDriver, error) {
	cfg := parsePostgresBackendConfig(uri)

	return &PostgresMonolithDriver{
		cfg:  cfg,
		keys: btree.NewMap[string, string](0),
	}, nil
}

// Name returns the driver name
func (*PostgresMonolithDriver) Name() string {
	return DriverName
}

// Health returns the health status of the driver by verifying database connectivity
func (d *PostgresMonolithDriver) Health() (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.pool == nil {
		return false, nil
	}

	// Verify database connection with ping
	if err := d.pool.Ping(context.Background()); err != nil {
		return false, err
	}

	return true, nil
}

// IsBusy checks if the driver is currently busy with operations
func (d *PostgresMonolithDriver) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the driver is busy
	if !d.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	d.mu.Unlock()
	return false
}

// GetCapabilities returns the capabilities supported by this driver
func (*PostgresMonolithDriver) GetCapabilities() *service.DriverCapabilities {
	return &service.DriverCapabilities{
		Type: DriverType,
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
				MaxObjectSize: 10485760, // 10 MB practical limit
				MaxPathLength: 255,
				CaseSensitive: true,
			},
		},
	}
}

// OpenDriver initializes the database connection and loads existing keys into memory
func (d *PostgresMonolithDriver) OpenDriver(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Parse connection string and create pool config
	config, err := pgxpool.ParseConfig(d.cfg.ConnString)
	if err != nil {
		return fmt.Errorf("invalid connection string: %w", err)
	}

	// Disable prepared statement caching to avoid collisions in pooled connections
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Initialize schema (split into individual statements to avoid cache collisions)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Execute schema creation
	if _, err := conn.Exec(ctx, postgresSchema); err != nil {
		pool.Close()
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Load all existing keys into B-tree for fast lookups
	rows, err := conn.Query(ctx, "SELECT namespace, key, id FROM vfs_metadata")
	if err != nil {
		pool.Close()
		return fmt.Errorf("failed to load keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var namespace, key, id string
		if err := rows.Scan(&namespace, &key, &id); err != nil {
			pool.Close()
			return fmt.Errorf("failed to scan key: %w", err)
		}

		// Use NamedKey helper to create namespaced key
		namedKey := service.NamedKey(namespace, key, ":")
		d.keys.Set(namedKey, id)
	}

	if err := rows.Err(); err != nil {
		pool.Close()
		return fmt.Errorf("failed to iterate keys: %w", err)
	}

	d.pool = pool
	return nil
}

// CloseDriver cleans up database connection and clears in-memory data
func (d *PostgresMonolithDriver) CloseDriver(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Clear the B-tree index
	d.keys.Clear()

	// Close database connection pool if it exists
	if d.pool != nil {
		d.pool.Close()
		d.pool = nil
	}

	return nil
}

// GetObjectStorageService returns the ObjectStorage service
func (d *PostgresMonolithDriver) GetObjectStorageService() service.ObjectStorageService {
	return &PostgresObjectStorageService{
		driver: d,
	}
}

// GetMetadataService returns the Metadata service
func (d *PostgresMonolithDriver) GetMetadataService() service.MetadataService {
	return &PostgresMetadataService{
		driver: d,
	}
}

// GetExtensionService returns the extension service (none supported for Postgres currently)
func (d *PostgresMonolithDriver) GetExtensionService(ext service.ServiceExtension) service.Service {
	return nil
}

// parsePostgresBackendConfig parses URI into Postgres configuration
func parsePostgresBackendConfig(uri *service.Uri) *PostgresBackendConfig {
	// Reconstruct connection string from URI components
	// postgres://user:pass@host:port/dbname?param=value

	// Build user info
	userInfo := ""
	if uri.Username != "" {
		userInfo = uri.Username
		if uri.Password != "" {
			userInfo += ":" + uri.Password
		}
		userInfo += "@"
	}

	// Build host and port
	hostPort := uri.Host
	if uri.Port > 0 {
		hostPort += fmt.Sprintf(":%d", uri.Port)
	}

	// Build query string
	queryStr := ""
	if len(uri.Query) > 0 {
		queryStr = "?"
		first := true
		for k, v := range uri.Query {
			if !first {
				queryStr += "&"
			}
			queryStr += k + "=" + v
			first = false
		}
	}

	connString := fmt.Sprintf("postgres://%s%s%s%s",
		userInfo, // user:pass@
		hostPort, // host:port
		uri.Path, // /dbname
		queryStr, // ?param=value
	)

	return &PostgresBackendConfig{
		ConnString: connString,
	}
}
