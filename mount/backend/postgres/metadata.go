package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/backend"
)

func (pb *PostgresBackend) CreateMeta(ctx context.Context, meta *data.Metadata) error {
	// Check if key already exists in B-tree
	if _, exists := pb.keys.Get(meta.Key); exists {
		return data.ErrExist
	}

	// Populate unique ID if not already defined
	if meta.ID == "" {
		// Use the helper from data package
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

	conn, err := pb.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	contentType := string(meta.ContentType)
	// Insert into database
	_, err = conn.Exec(ctx, `
		INSERT INTO vfs_metadata (id, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, meta.ID, meta.Key, int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(), meta.AccessTime.Unix(), meta.CreateTime.Unix(),
		nullString(contentType), nullString(meta.ETag), attributesJSON)

	if err != nil {
		return fmt.Errorf("failed to insert metadata: %w", err)
	}

	// Update B-tree
	pb.keys.Set(meta.Key, meta.ID)
	return nil
}

func (pb *PostgresBackend) ReadMeta(ctx context.Context, key string) (*data.Metadata, error) {
	// Check B-tree first
	id, exists := pb.keys.Get(key)
	if !exists {
		return nil, data.ErrNotExist
	}

	conn, err := pb.pool.Acquire(ctx)
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

	err = conn.QueryRow(ctx, `
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

func (pb *PostgresBackend) UpdateMeta(ctx context.Context, key string, update *data.MetadataUpdate) error {
	// Check if key exists
	id, exists := pb.keys.Get(key)
	if !exists {
		return data.ErrNotExist
	}

	// Read current metadata
	meta, err := pb.ReadMeta(ctx, key)
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

	conn, err := pb.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	contentType := string(meta.ContentType)
	// Update database
	_, err = conn.Exec(ctx, `
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

func (pb *PostgresBackend) DeleteMeta(ctx context.Context, key string) error {
	// Check if key exists
	id, exists := pb.keys.Get(key)
	if !exists {
		return data.ErrNotExist
	}

	conn, err := pb.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Start transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Delete metadata
	_, err = tx.Exec(ctx, "DELETE FROM vfs_metadata WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}

	// Decrement ref count for associated data
	_, err = tx.Exec(ctx, `
		UPDATE vfs_data
		SET ref_count = ref_count - 1
		WHERE id = $1
	`, id)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to update ref count: %w", err)
	}

	// Delete data with ref_count = 0
	_, err = tx.Exec(ctx, "DELETE FROM vfs_data WHERE ref_count <= 0")
	if err != nil {
		return fmt.Errorf("failed to cleanup data: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Remove from B-tree
	pb.keys.Delete(key)
	return nil
}

func (pb *PostgresBackend) ExistsMeta(ctx context.Context, key string) (bool, error) {
	_, exists := pb.keys.Get(key)
	return exists, nil
}

func (pb *PostgresBackend) QueryMeta(ctx context.Context, query *backend.MetadataQuery) (*backend.MetadataQueryResult, error) {
	// Build dynamic SQL query
	sqlQuery := "SELECT id, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes FROM vfs_metadata WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	// Prefix and delimiter filter
	if query.Delimiter == "/" {
		if query.Prefix != "" {
			// Only immediate children under prefix - exclude nested paths
			sqlQuery += fmt.Sprintf(" AND key LIKE $%d AND key NOT LIKE $%d", argIdx, argIdx+1)
			args = append(args, query.Prefix+"%", query.Prefix+"%/%")
			argIdx += 2
		} else {
			// Root level only - no slashes in key
			sqlQuery += fmt.Sprintf(" AND key NOT LIKE $%d", argIdx)
			args = append(args, "%/%")
			argIdx++
		}
	} else {
		// No delimiter: recursive listing
		if query.Prefix != "" {
			// Recursive - include all descendants matching prefix
			sqlQuery += fmt.Sprintf(" AND key LIKE $%d", argIdx)
			args = append(args, query.Prefix+"%")
			argIdx++
		}
		// If no prefix and no delimiter, return all (no additional filter)
	}

	// Content type filter
	if query.ContentType != nil {
		if strings.Contains(*query.ContentType, "*") {
			// Wildcard matching: "image/*"
			pattern := strings.Replace(*query.ContentType, "*", "%", -1)
			sqlQuery += fmt.Sprintf(" AND content_type LIKE $%d", argIdx)
			args = append(args, pattern)
			argIdx++
		} else {
			sqlQuery += fmt.Sprintf(" AND content_type = $%d", argIdx)
			args = append(args, *query.ContentType)
			argIdx++
		}
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

	// Sorting
	if query.SortBy != "" {
		sqlQuery += fmt.Sprintf(" ORDER BY %s %s", query.SortBy, query.SortOrder)
	}

	// Pagination
	if query.Limit > 0 {
		sqlQuery += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
		args = append(args, query.Limit, query.Offset)
		argIdx += 2
	}

	conn, err := pb.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Execute query
	rows, err := conn.Query(ctx, sqlQuery, args...)
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

	// Return results
	return &backend.MetadataQueryResult{
		Candidates: results,
		TotalCount: len(results),
		Paginating: query.Limit > 0 && len(results) == query.Limit,
	}, nil
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
