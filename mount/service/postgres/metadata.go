package postgres

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

// GetLifecycle returns the driver's lifecycle interface
func (s *PostgresMetadataService) GetLifecycle() service.Lifecycle {
	return s.driver
}

// CreateMeta creates new metadata for an object
func (s *PostgresMetadataService) CreateMeta(traversal context.TraversalContext, ns string, meta *data.Metadata) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Check if key already exists in B-tree
	namedKey := service.NamedKey(ns, meta.Key, ":")
	if _, exists := s.driver.keys.Get(namedKey); exists {
		return data.ErrExist
	}

	// Populate unique ID if not already defined
	if meta.ID == "" {
		tempMeta := data.NewMetadata(meta.Key, meta.Mode, meta.Size)
		meta.ID = tempMeta.ID
	}

	// Update CreateTime if not set
	if meta.CreateTime.IsZero() {
		meta.CreateTime = time.Now()
	}

	// Serialize attributes to JSONB
	var attributesJSON []byte
	if len(meta.Attributes) > 0 {
		var err error
		attributesJSON, err = json.Marshal(meta.Attributes)
		if err != nil {
			return fmt.Errorf("failed to marshal attributes: %w", err)
		}
	}

	conn, err := s.driver.pool.Acquire(traversal)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	contentType := string(meta.ContentType)

	// Insert into database
	_, err = conn.Exec(traversal, `
		INSERT INTO vfs_metadata (id, namespace, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`, meta.ID, ns, meta.Key, int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(), meta.AccessTime.Unix(), meta.CreateTime.Unix(),
		nullString(contentType), nullString(meta.ETag), attributesJSON)

	if err != nil {
		return fmt.Errorf("failed to insert metadata: %w", err)
	}

	// Update B-tree
	s.driver.keys.Set(namedKey, meta.ID)
	return nil
}

// ReadMeta reads metadata for an object
func (s *PostgresMetadataService) ReadMeta(traversal context.TraversalContext, ns, key string) (*data.Metadata, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Check B-tree first
	namedKey := service.NamedKey(ns, key, ":")
	id, exists := s.driver.keys.Get(namedKey)
	if !exists {
		return nil, data.ErrNotExist
	}

	conn, err := s.driver.pool.Acquire(traversal)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Query database
	var meta data.Metadata
	var uid, gid *int64
	var contentType, etag *string
	var attributesJSON []byte
	var modifyTime, accessTime, createTime int64

	err = conn.QueryRow(traversal, `
		SELECT id, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes
		FROM vfs_metadata WHERE id = $1
	`, id).Scan(&meta.ID, &meta.Key, &meta.Mode, &meta.Size,
		&uid, &gid, &modifyTime, &accessTime, &createTime,
		&contentType, &etag, &attributesJSON)

	if err == pgx.ErrNoRows {
		return nil, data.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query metadata: %w", err)
	}

	// Convert timestamps
	meta.ModifyTime = time.Unix(modifyTime, 0)
	meta.AccessTime = time.Unix(accessTime, 0)
	meta.CreateTime = time.Unix(createTime, 0)

	// Convert nullable fields
	if uid != nil {
		meta.UID = *uid
	}
	if gid != nil {
		meta.GID = *gid
	}
	if contentType != nil {
		meta.ContentType = data.ContentType(*contentType)
	}
	if etag != nil {
		meta.ETag = *etag
	}

	// Deserialize attributes
	if len(attributesJSON) > 0 {
		if err := json.Unmarshal(attributesJSON, &meta.Attributes); err != nil {
			meta.Attributes = make(map[string]string)
		}
	} else {
		meta.Attributes = make(map[string]string)
	}

	// Update access time (in memory only for performance)
	meta.AccessTime = time.Now()

	return &meta, nil
}

// UpdateMeta updates metadata for an object
func (s *PostgresMetadataService) UpdateMeta(traversal context.TraversalContext, ns, key string, update *data.MetadataUpdate) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Check if key exists
	namedKey := service.NamedKey(ns, key, ":")
	id, exists := s.driver.keys.Get(namedKey)
	if !exists {
		return data.ErrNotExist
	}

	// Read current metadata
	meta, err := s.readMetaInternal(traversal, id)
	if err != nil {
		return err
	}

	// Apply update
	meta.ModifyTime = time.Now()
	if _, err := update.Apply(meta); err != nil {
		return fmt.Errorf("failed to apply update: %w", err)
	}

	// Serialize attributes to JSONB
	var attributesJSON []byte
	if len(meta.Attributes) > 0 {
		attributesJSON, err = json.Marshal(meta.Attributes)
		if err != nil {
			return fmt.Errorf("failed to marshal attributes: %w", err)
		}
	}

	conn, err := s.driver.pool.Acquire(traversal)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	contentType := string(meta.ContentType)

	// Update database
	_, err = conn.Exec(traversal, `
		UPDATE vfs_metadata
		SET mode = $1, size = $2, uid = $3, gid = $4,
		    modify_time = $5, access_time = $6, content_type = $7, etag = $8, attributes = $9
		WHERE id = $10
	`, int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(), meta.AccessTime.Unix(),
		nullString(contentType), nullString(meta.ETag), attributesJSON, id)

	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	return nil
}

// ExistsMeta checks if metadata exists for an object
func (s *PostgresMetadataService) ExistsMeta(traversal context.TraversalContext, ns, key string) (bool, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Check B-tree for key existence
	namedKey := service.NamedKey(ns, key, ":")
	_, exists := s.driver.keys.Get(namedKey)
	return exists, nil
}

// DeleteMeta deletes metadata for an object
func (s *PostgresMetadataService) DeleteMeta(traversal context.TraversalContext, ns, key string) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Check if key exists
	namedKey := service.NamedKey(ns, key, ":")
	id, exists := s.driver.keys.Get(namedKey)
	if !exists {
		return data.ErrNotExist
	}

	conn, err := s.driver.pool.Acquire(traversal)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Start transaction
	tx, err := conn.Begin(traversal)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(traversal)

	// Delete metadata
	_, err = tx.Exec(traversal, "DELETE FROM vfs_metadata WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}

	// Decrement ref count for associated data
	_, err = tx.Exec(traversal, `
		UPDATE vfs_data SET ref_count = ref_count - 1 WHERE id = $1
	`, id)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to update ref count: %w", err)
	}

	// Delete data with ref_count <= 0
	_, err = tx.Exec(traversal, "DELETE FROM vfs_data WHERE ref_count <= 0")
	if err != nil {
		return fmt.Errorf("failed to cleanup data: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(traversal); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Remove from B-tree
	s.driver.keys.Delete(namedKey)
	return nil
}

// QueryMeta queries metadata with filters, sorting, and pagination
func (s *PostgresMetadataService) QueryMeta(traversal context.TraversalContext, ns string, query *service.Query) (*service.QueryPagination, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Build dynamic SQL query
	sqlQuery := "SELECT id, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes FROM vfs_metadata WHERE namespace = $1"
	args := []interface{}{ns}
	argIdx := 2

	// Prefix filter
	if query.Prefix != "" {
		sqlQuery += fmt.Sprintf(" AND key LIKE $%d", argIdx)
		args = append(args, query.Prefix+"%")
		argIdx++
	}

	// Content type filter
	if query.ContentType != nil {
		if strings.Contains(*query.ContentType, "*") {
			// Wildcard matching: "image/*"
			pattern := strings.Replace(*query.ContentType, "*", "%", -1)
			sqlQuery += fmt.Sprintf(" AND content_type LIKE $%d", argIdx)
			args = append(args, pattern)
		} else {
			sqlQuery += fmt.Sprintf(" AND content_type = $%d", argIdx)
			args = append(args, *query.ContentType)
		}
		argIdx++
	}

	// File type filter (check mode bits)
	if query.FilterType != nil {
		switch *query.FilterType {
		case data.FileTypeDir:
			sqlQuery += fmt.Sprintf(" AND (mode & $%d) != 0", argIdx)
			args = append(args, data.ModeDir)
			argIdx++
		case data.FileTypeRegular:
			// Regular files have no type bits set
			allTypeBits := data.ModeDir | data.ModeSymlink | data.ModeNamedPipe | data.ModeSocket | data.ModeDevice | data.ModeCharDevice | data.ModeIrregular
			sqlQuery += fmt.Sprintf(" AND (mode & $%d) = 0", argIdx)
			args = append(args, allTypeBits)
			argIdx++
		}
	}

	// Size filters
	if query.MinSize != nil {
		sqlQuery += fmt.Sprintf(" AND size >= $%d", argIdx)
		args = append(args, *query.MinSize)
		argIdx++
	}
	if query.MaxSize != nil {
		sqlQuery += fmt.Sprintf(" AND size <= $%d", argIdx)
		args = append(args, *query.MaxSize)
		argIdx++
	}

	// Sorting (whitelist allowed fields for security)
	if query.SortBy != "" {
		allowedSortFields := map[string]bool{
			"key": true, "size": true, "modify_time": true,
			"access_time": true, "create_time": true,
		}
		sortByStr := string(query.SortBy)
		if allowedSortFields[sortByStr] {
			sortOrder := "ASC"
			if query.SortOrder == service.SortDesc {
				sortOrder = "DESC"
			}
			sqlQuery += fmt.Sprintf(" ORDER BY %s %s", sortByStr, sortOrder)
		}
	}

	// Pagination
	if query.Limit > 0 {
		sqlQuery += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
		args = append(args, query.Limit, query.Offset)
	}

	conn, err := s.driver.pool.Acquire(traversal)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Execute query
	rows, err := conn.Query(traversal, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Process results
	results := make([]*data.Metadata, 0)
	for rows.Next() {
		var meta data.Metadata
		var uid, gid *int64
		var contentType, etag *string
		var attributesJSON []byte
		var modifyTime, accessTime, createTime int64

		err := rows.Scan(&meta.ID, &meta.Key, &meta.Mode, &meta.Size,
			&uid, &gid, &modifyTime, &accessTime, &createTime,
			&contentType, &etag, &attributesJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert timestamps
		meta.ModifyTime = time.Unix(modifyTime, 0)
		meta.AccessTime = time.Unix(accessTime, 0)
		meta.CreateTime = time.Unix(createTime, 0)

		// Convert nullable fields
		if uid != nil {
			meta.UID = *uid
		}
		if gid != nil {
			meta.GID = *gid
		}
		if contentType != nil {
			meta.ContentType = data.ContentType(*contentType)
		}
		if etag != nil {
			meta.ETag = *etag
		}

		// Deserialize attributes
		if len(attributesJSON) > 0 {
			if err := json.Unmarshal(attributesJSON, &meta.Attributes); err != nil {
				meta.Attributes = make(map[string]string)
			}
		} else {
			meta.Attributes = make(map[string]string)
		}

		results = append(results, &meta)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate rows: %w", err)
	}

	// Return results with pagination info
	return &service.QueryPagination{
		Candidates: results,
		TotalCount: len(results),
		Paginating: query.Limit > 0 && len(results) == query.Limit,
	}, nil
}

// readMetaInternal reads metadata by ID without locking (caller must hold lock)
func (s *PostgresMetadataService) readMetaInternal(traversal context.TraversalContext, id string) (*data.Metadata, error) {
	conn, err := s.driver.pool.Acquire(traversal)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var meta data.Metadata
	var uid, gid *int64
	var contentType, etag *string
	var attributesJSON []byte
	var modifyTime, accessTime, createTime int64

	err = conn.QueryRow(traversal, `
		SELECT id, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes
		FROM vfs_metadata WHERE id = $1
	`, id).Scan(&meta.ID, &meta.Key, &meta.Mode, &meta.Size,
		&uid, &gid, &modifyTime, &accessTime, &createTime,
		&contentType, &etag, &attributesJSON)

	if err == pgx.ErrNoRows {
		return nil, data.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query metadata: %w", err)
	}

	// Convert timestamps
	meta.ModifyTime = time.Unix(modifyTime, 0)
	meta.AccessTime = time.Unix(accessTime, 0)
	meta.CreateTime = time.Unix(createTime, 0)

	// Convert nullable fields
	if uid != nil {
		meta.UID = *uid
	}
	if gid != nil {
		meta.GID = *gid
	}
	if contentType != nil {
		meta.ContentType = data.ContentType(*contentType)
	}
	if etag != nil {
		meta.ETag = *etag
	}

	// Deserialize attributes
	if len(attributesJSON) > 0 {
		if err := json.Unmarshal(attributesJSON, &meta.Attributes); err != nil {
			meta.Attributes = make(map[string]string)
		}
	} else {
		meta.Attributes = make(map[string]string)
	}

	return &meta, nil
}

// Helper functions for nullable fields
func nullInt64(val int64) *int64 {
	if val == 0 {
		return nil
	}
	return &val
}

func nullString(val string) *string {
	if val == "" {
		return nil
	}
	return &val
}
