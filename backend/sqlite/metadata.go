package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/mwantia/vfs/data"
)

func (sb *SQLiteBackend) CreateMeta(ctx context.Context, meta *data.VirtualFileMetadata) error {
	// Check if key already exists in B-tree
	if _, exists := sb.keys.Get(meta.Key); exists {
		return data.ErrExist
	}

	// Populate unique ID if not already defined
	if meta.ID == "" {
		// Use the helper from data package
		tempMeta := data.NewMetadata(meta.Key, meta.Type, meta.Mode, meta.Size)
		meta.ID = tempMeta.ID
	}

	// Update CreateTime if not set
	if meta.CreateTime.IsZero() {
		meta.CreateTime = time.Now()
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

	// Insert into database
	_, err := sb.db.ExecContext(ctx, `
		INSERT INTO vfs_metadata (id, key, type, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, meta.ID, meta.Key, int(meta.Type), int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(), meta.AccessTime.Unix(), meta.CreateTime.Unix(),
		nullString(meta.ContentType), nullString(meta.ETag), attributesJSON)

	if err != nil {
		return err
	}

	// Update B-tree
	sb.keys.Set(meta.Key, meta.ID)
	return nil
}

func (sb *SQLiteBackend) ReadMeta(ctx context.Context, key string) (*data.VirtualFileMetadata, error) {
	// Check B-tree first
	id, exists := sb.keys.Get(key)
	if !exists {
		return nil, data.ErrNotExist
	}

	// Query database
	var meta data.VirtualFileMetadata
	var uid, gid sql.NullInt64
	var contentType, etag sql.NullString
	var attributesJSON sql.NullString
	var modifyTime, accessTime, createTime int64

	err := sb.db.QueryRowContext(ctx, `
		SELECT id, key, type, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes
		FROM vfs_metadata WHERE id = ?
	`, id).Scan(&meta.ID, &meta.Key, &meta.Type, &meta.Mode, &meta.Size,
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
		meta.ContentType = contentType.String
	}
	if etag.Valid {
		meta.ETag = etag.String
	}

	// Deserialize attributes
	if attributesJSON.Valid && attributesJSON.String != "" {
		if err := json.Unmarshal([]byte(attributesJSON.String), &meta.Attributes); err != nil {
			meta.Attributes = make(map[string]string)
		}
	} else {
		meta.Attributes = make(map[string]string)
	}

	// Update access time (in memory only for performance)
	meta.AccessTime = time.Now()

	return &meta, nil
}

func (sb *SQLiteBackend) UpdateMeta(ctx context.Context, key string, update *data.VirtualFileMetadataUpdate) error {
	// Check if key exists
	id, exists := sb.keys.Get(key)
	if !exists {
		return data.ErrNotExist
	}

	// Read current metadata
	meta, err := sb.ReadMeta(ctx, key)
	if err != nil {
		return err
	}

	// Apply update
	meta.ModifyTime = time.Now()
	if _, err := update.Apply(meta); err != nil {
		return err
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

	// Update database
	_, err = sb.db.ExecContext(ctx, `
		UPDATE vfs_metadata
		SET type = ?, mode = ?, size = ?, uid = ?, gid = ?,
		    modify_time = ?, access_time = ?, content_type = ?, etag = ?, attributes = ?
		WHERE id = ?
	`, int(meta.Type), int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(), meta.AccessTime.Unix(),
		nullString(meta.ContentType), nullString(meta.ETag), attributesJSON, id)

	return err
}

func (sb *SQLiteBackend) DeleteMeta(ctx context.Context, key string) error {
	// Check if key exists
	id, exists := sb.keys.Get(key)
	if !exists {
		return data.ErrNotExist
	}

	// Start transaction
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete metadata
	_, err = tx.ExecContext(ctx, "DELETE FROM vfs_metadata WHERE id = ?", id)
	if err != nil {
		return err
	}

	// Check if there's associated data and decrement ref count
	_, err = tx.ExecContext(ctx, `
		UPDATE vfs_data
		SET ref_count = ref_count - 1
		WHERE id = (SELECT id FROM vfs_data WHERE id = ?)
	`, id)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Delete data with ref_count = 0
	_, err = tx.ExecContext(ctx, "DELETE FROM vfs_data WHERE ref_count = 0")
	if err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	// Remove from B-tree
	sb.keys.Delete(key)
	return nil
}

func (sb *SQLiteBackend) ExistsMeta(ctx context.Context, key string) (bool, error) {
	_, exists := sb.keys.Get(key)
	return exists, nil
}

func (sb *SQLiteBackend) ReadAllMeta(ctx context.Context) ([]*data.VirtualFileMetadata, error) {
	rows, err := sb.db.QueryContext(ctx, `
		SELECT id, key, type, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes
		FROM vfs_metadata
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metas []*data.VirtualFileMetadata
	for rows.Next() {
		var meta data.VirtualFileMetadata
		var uid, gid sql.NullInt64
		var contentType, etag sql.NullString
		var attributesJSON sql.NullString
		var modifyTime, accessTime, createTime int64

		err := rows.Scan(&meta.ID, &meta.Key, &meta.Type, &meta.Mode, &meta.Size,
			&uid, &gid, &modifyTime, &accessTime, &createTime,
			&contentType, &etag, &attributesJSON)
		if err != nil {
			continue
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
			meta.ContentType = contentType.String
		}
		if etag.Valid {
			meta.ETag = etag.String
		}

		// Deserialize attributes
		if attributesJSON.Valid && attributesJSON.String != "" {
			if err := json.Unmarshal([]byte(attributesJSON.String), &meta.Attributes); err != nil {
				meta.Attributes = make(map[string]string)
			}
		} else {
			meta.Attributes = make(map[string]string)
		}

		metas = append(metas, &meta)
	}

	return metas, rows.Err()
}

func (sb *SQLiteBackend) CreateAllMeta(ctx context.Context, metas []*data.VirtualFileMetadata) error {
	errs := data.Errors{}

	for _, meta := range metas {
		if err := sb.CreateMeta(ctx, meta); err != nil {
			errs.Add(err)
		}
	}

	return errs.Errors()
}

// Helper functions for nullable fields
func nullInt64(val int64) sql.NullInt64 {
	if val == 0 {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: val, Valid: true}
}

func nullString(val string) sql.NullString {
	if val == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: val, Valid: true}
}
