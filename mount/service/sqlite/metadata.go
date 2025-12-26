package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

func (s *SqliteMetadataService) GetLifecycle() service.Lifecycle {
	return s.driver
}

func (s *SqliteMetadataService) ExistsMeta(traversal context.TraversalContext, ns, key string) (bool, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	namedKey := service.NamedKey(ns, key, ":")
	_, exists := s.driver.keys.Get(namedKey)
	return exists, nil
}

func (s *SqliteMetadataService) ReadMeta(traversal context.TraversalContext, ns, key string) (*data.Metadata, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Check B-tree for key existence
	namedKey := service.NamedKey(ns, key, ":")
	id, exists := s.driver.keys.Get(namedKey)
	if !exists {
		return nil, data.ErrNotExist
	}

	// Query full metadata from database
	var meta data.Metadata
	var uid, gid sql.NullInt64
	var contentType, etag sql.NullString
	var attributesJSON sql.NullString
	var modifyTime, accessTime, createTime int64

	err := s.driver.db.QueryRowContext(traversal, `
		SELECT id, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes
		FROM vfs_metadata WHERE id = ?
	`, id).Scan(&meta.ID, &meta.Key, &meta.Mode, &meta.Size,
		&uid, &gid, &modifyTime, &accessTime, &createTime,
		&contentType, &etag, &attributesJSON)

	if err == sql.ErrNoRows {
		return nil, data.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	// Convert timestamps
	meta.ModifyTime = time.Unix(modifyTime, 0)
	meta.AccessTime = time.Unix(accessTime, 0)
	meta.CreateTime = time.Unix(createTime, 0)

	// Convert nullable fields
	if uid.Valid {
		meta.UID = uid.Int64
	}
	if gid.Valid {
		meta.GID = gid.Int64
	}
	if contentType.Valid {
		meta.ContentType = data.ContentType(contentType.String)
	}
	if etag.Valid {
		meta.ETag = etag.String
	}

	// Deserialize attributes from JSON
	if attributesJSON.Valid && attributesJSON.String != "" {
		if err := json.Unmarshal([]byte(attributesJSON.String), &meta.Attributes); err != nil {
			return nil, err
		}
	}

	return &meta, nil
}

func (s *SqliteMetadataService) CreateMeta(traversal context.TraversalContext, ns string, meta *data.Metadata) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Check if key already exists in B-tree
	namedKey := service.NamedKey(ns, meta.Key, ":")
	if _, exists := s.driver.keys.Get(namedKey); exists {
		return data.ErrExist
	}

	// Generate ID if not provided
	if meta.ID == "" {
		tempMeta := data.NewMetadata(meta.Key, meta.Mode, meta.Size)
		meta.ID = tempMeta.ID
	}

	// Set timestamps if not already set
	now := time.Now()
	if meta.CreateTime.IsZero() {
		meta.CreateTime = now
	}
	if meta.ModifyTime.IsZero() {
		meta.ModifyTime = now
	}
	if meta.AccessTime.IsZero() {
		meta.AccessTime = now
	}

	// Serialize attributes to JSON
	var attributesJSON sql.NullString
	if len(meta.Attributes) > 0 {
		bytes, err := json.Marshal(meta.Attributes)
		if err != nil {
			return err
		}
		attributesJSON = sql.NullString{String: string(bytes), Valid: true}
	}

	contentType := string(meta.ContentType)

	// Insert into database
	_, err := s.driver.db.ExecContext(traversal, `
		INSERT INTO vfs_metadata (id, namespace, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, meta.ID, ns, meta.Key, int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(), meta.AccessTime.Unix(), meta.CreateTime.Unix(),
		nullString(contentType), nullString(meta.ETag), attributesJSON)

	if err != nil {
		return err
	}

	// Update B-tree with namespaced key
	s.driver.keys.Set(namedKey, meta.ID)

	return nil
}

func (s *SqliteMetadataService) UpdateMeta(traversal context.TraversalContext, ns, key string, update *data.MetadataUpdate) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Check if key exists
	namedKey := service.NamedKey(ns, key, ":")
	id, exists := s.driver.keys.Get(namedKey)
	if !exists {
		return data.ErrNotExist
	}

	// Read current metadata
	var meta data.Metadata
	var uid, gid sql.NullInt64
	var contentType, etag sql.NullString
	var attributesJSON sql.NullString
	var modifyTime, accessTime, createTime int64

	err := s.driver.db.QueryRowContext(traversal, `
		SELECT id, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes
		FROM vfs_metadata WHERE id = ?
	`, id).Scan(&meta.ID, &meta.Key, &meta.Mode, &meta.Size,
		&uid, &gid, &modifyTime, &accessTime, &createTime,
		&contentType, &etag, &attributesJSON)

	if err != nil {
		return err
	}

	// Convert timestamps
	meta.ModifyTime = time.Unix(modifyTime, 0)
	meta.AccessTime = time.Unix(accessTime, 0)
	meta.CreateTime = time.Unix(createTime, 0)

	// Convert nullable fields
	if uid.Valid {
		meta.UID = uid.Int64
	}
	if gid.Valid {
		meta.GID = gid.Int64
	}
	if contentType.Valid {
		meta.ContentType = data.ContentType(contentType.String)
	}
	if etag.Valid {
		meta.ETag = etag.String
	}

	// Deserialize attributes from JSON
	if attributesJSON.Valid && attributesJSON.String != "" {
		if err := json.Unmarshal([]byte(attributesJSON.String), &meta.Attributes); err != nil {
			return err
		}
	}

	// Apply update and set ModifyTime
	meta.ModifyTime = time.Now()
	if _, err := update.Apply(&meta); err != nil {
		return err
	}

	// Serialize attributes back to JSON
	var newAttributesJSON sql.NullString
	if len(meta.Attributes) > 0 {
		bytes, err := json.Marshal(meta.Attributes)
		if err != nil {
			return err
		}
		newAttributesJSON = sql.NullString{String: string(bytes), Valid: true}
	}

	newContentType := string(meta.ContentType)

	// Update in database
	_, err = s.driver.db.ExecContext(traversal, `
		UPDATE vfs_metadata
		SET key = ?, mode = ?, size = ?, uid = ?, gid = ?,
		    modify_time = ?, content_type = ?, etag = ?, attributes = ?
		WHERE id = ?
	`, meta.Key, int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(),
		nullString(newContentType), nullString(meta.ETag), newAttributesJSON,
		id)

	return err
}

func (s *SqliteMetadataService) DeleteMeta(traversal context.TraversalContext, ns, key string) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Check if key exists
	namedKey := service.NamedKey(ns, key, ":")
	id, exists := s.driver.keys.Get(namedKey)
	if !exists {
		return data.ErrNotExist
	}

	// Start transaction for atomic deletion
	tx, err := s.driver.db.BeginTx(traversal, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete metadata
	_, err = tx.ExecContext(traversal, "DELETE FROM vfs_metadata WHERE id = ?", id)
	if err != nil {
		return err
	}

	// Decrement ref_count for associated data
	_, err = tx.ExecContext(traversal, `
		UPDATE vfs_data
		SET ref_count = ref_count - 1
		WHERE id = ?
	`, id)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Delete data rows with ref_count = 0 (cleanup unreferenced data)
	_, err = tx.ExecContext(traversal, "DELETE FROM vfs_data WHERE ref_count = 0")
	if err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	// Remove from B-tree index
	s.driver.keys.Delete(namedKey)

	return nil
}

func (s *SqliteMetadataService) QueryMeta(traversal context.TraversalContext, ns string, query *service.Query) (*service.QueryPagination, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Build dynamic SQL query
	sqlQuery := "SELECT id, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes FROM vfs_metadata WHERE namespace = ?"
	args := []interface{}{ns}

	// Prefix and delimiter filter
	if query.Delimiter == "/" {
		if query.Prefix != "" {
			// Only immediate children under prefix - exclude nested paths
			sqlQuery += " AND key LIKE ? AND key NOT LIKE ?"
			args = append(args, query.Prefix+"%", query.Prefix+"%/%")
		} else {
			// Root level only - no slashes in key
			sqlQuery += " AND key NOT LIKE ?"
			args = append(args, "%/%")
		}
	} else {
		// No delimiter: recursive listing
		if query.Prefix != "" {
			// Recursive - include all descendants matching prefix
			sqlQuery += " AND key LIKE ?"
			args = append(args, query.Prefix+"%")
		}
		// If no prefix and no delimiter, return all (no additional filter)
	}

	// Content type filter
	if query.ContentType != nil {
		if strings.Contains(*query.ContentType, "*") {
			// Wildcard matching: "image/*", "*/json", "*/*"
			pattern := strings.Replace(*query.ContentType, "*", "%", -1)
			sqlQuery += " AND content_type LIKE ?"
			args = append(args, pattern)
		} else {
			sqlQuery += " AND content_type = ?"
			args = append(args, *query.ContentType)
		}
	}

	// File type filter (check mode bits)
	if query.FilterType != nil {
		switch *query.FilterType {
		case data.FileTypeDir:
			sqlQuery += " AND (mode & ?) != 0"
			args = append(args, data.ModeDir)
		case data.FileTypeRegular:
			// Regular files have no type bits set
			allTypeBits := data.ModeDir | data.ModeSymlink | data.ModeNamedPipe | data.ModeSocket | data.ModeDevice | data.ModeCharDevice | data.ModeIrregular
			sqlQuery += " AND (mode & ?) = 0"
			args = append(args, allTypeBits)
		}
	}

	// Size filters
	if query.MinSize != nil {
		sqlQuery += " AND size >= ?"
		args = append(args, *query.MinSize)
	}
	if query.MaxSize != nil {
		sqlQuery += " AND size <= ?"
		args = append(args, *query.MaxSize)
	}

	// Sorting
	if query.SortBy != "" {
		sqlQuery += fmt.Sprintf(" ORDER BY %s %s", query.SortBy, query.SortOrder)
	}

	// Pagination
	if query.Limit > 0 {
		sqlQuery += " LIMIT ? OFFSET ?"
		args = append(args, query.Limit, query.Offset)
	}

	// Execute query
	rows, err := s.driver.db.QueryContext(traversal, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Process results
	results := make([]*data.Metadata, 0)
	for rows.Next() {
		var meta data.Metadata
		var uid, gid sql.NullInt64
		var contentType, etag sql.NullString
		var attributesJSON sql.NullString
		var modifyTime, accessTime, createTime int64

		err := rows.Scan(&meta.ID, &meta.Key, &meta.Mode, &meta.Size,
			&uid, &gid, &modifyTime, &accessTime, &createTime,
			&contentType, &etag, &attributesJSON)
		if err != nil {
			return nil, err
		}

		// Convert timestamps
		meta.ModifyTime = time.Unix(modifyTime, 0)
		meta.AccessTime = time.Unix(accessTime, 0)
		meta.CreateTime = time.Unix(createTime, 0)

		// Convert nullable fields
		if uid.Valid {
			meta.UID = uid.Int64
		}
		if gid.Valid {
			meta.GID = gid.Int64
		}
		if contentType.Valid {
			meta.ContentType = data.ContentType(contentType.String)
		}
		if etag.Valid {
			meta.ETag = etag.String
		}

		// Deserialize attributes from JSON
		if attributesJSON.Valid && attributesJSON.String != "" {
			if err := json.Unmarshal([]byte(attributesJSON.String), &meta.Attributes); err != nil {
				// Skip this entry on deserialization error
				continue
			}
		}

		results = append(results, &meta)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &service.QueryPagination{
		Candidates: results,
		TotalCount: len(results),
		Paginating: query.Limit > 0,
	}, nil
}

// Helper functions for nullable SQL values
func nullInt64(v int64) sql.NullInt64 {
	if v == 0 {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: v, Valid: true}
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}
