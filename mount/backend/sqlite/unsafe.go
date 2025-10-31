package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/backend"
)

// This file contains internal "unsafe" methods that perform operations without acquiring locks.
// These methods MUST only be called when the caller already holds the appropriate lock.
// They are used by both public API methods (which acquire locks) and internal methods
// (like storage operations that already hold locks).

// createMetaUnsafe creates metadata without acquiring locks.
// MUST be called while holding a write lock.
func (sb *SQLiteBackend) createMetaUnsafe(ctx context.Context, namespace string, meta *data.Metadata) error {
	// Check if key already exists in B-tree
	nsKey := backend.NamespacedKey(namespace, meta.Key)
	if _, exists := sb.keys.Get(nsKey); exists {
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
	_, err := sb.db.ExecContext(ctx, `
		INSERT INTO vfs_metadata (id, namespace, key, mode, size, uid, gid, modify_time, access_time, create_time, content_type, etag, attributes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, meta.ID, namespace, meta.Key, int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(), meta.AccessTime.Unix(), meta.CreateTime.Unix(),
		nullString(contentType), nullString(meta.ETag), attributesJSON)

	if err != nil {
		return err
	}

	// Update B-tree with namespaced key
	sb.keys.Set(nsKey, meta.ID)
	return nil
}

// readMetaUnsafe reads metadata without acquiring locks.
// MUST be called while holding at least a read lock.
func (sb *SQLiteBackend) readMetaUnsafe(ctx context.Context, namespace string, key string) (*data.Metadata, error) {
	// Check B-tree first
	nsKey := backend.NamespacedKey(namespace, key)
	id, exists := sb.keys.Get(nsKey)
	if !exists {
		return nil, data.ErrNotExist
	}

	// Query database
	var meta data.Metadata
	var uid, gid sql.NullInt64
	var contentType, etag sql.NullString
	var attributesJSON sql.NullString
	var modifyTime, accessTime, createTime int64

	err := sb.db.QueryRowContext(ctx, `
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

// updateMetaUnsafe updates metadata without acquiring locks.
// MUST be called while holding a write lock.
func (sb *SQLiteBackend) updateMetaUnsafe(ctx context.Context, namespace string, key string, update *data.MetadataUpdate) error {
	// Check if key exists
	nsKey := backend.NamespacedKey(namespace, key)
	id, exists := sb.keys.Get(nsKey)
	if !exists {
		return data.ErrNotExist
	}

	// Read current metadata
	meta, err := sb.readMetaUnsafe(ctx, namespace, key)
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

	contentType := string(meta.ContentType)
	// Update database
	_, err = sb.db.ExecContext(ctx, `
		UPDATE vfs_metadata
		SET mode = ?, size = ?, uid = ?, gid = ?,
		    modify_time = ?, access_time = ?, content_type = ?, etag = ?, attributes = ?
		WHERE id = ?
	`, int(meta.Mode), meta.Size,
		nullInt64(meta.UID), nullInt64(meta.GID),
		meta.ModifyTime.Unix(), meta.AccessTime.Unix(),
		nullString(contentType), nullString(meta.ETag), attributesJSON, id)

	return err
}

// deleteMetaUnsafe deletes metadata without acquiring locks.
// MUST be called while holding a write lock.
func (sb *SQLiteBackend) deleteMetaUnsafe(ctx context.Context, namespace string, key string) error {
	// Check if key exists
	nsKey := backend.NamespacedKey(namespace, key)
	id, exists := sb.keys.Get(nsKey)
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

	// Remove from B-tree using namespaced key
	sb.keys.Delete(nsKey)
	return nil
}

// existsMetaUnsafe checks if metadata exists without acquiring locks.
// MUST be called while holding at least a read lock.
func (sb *SQLiteBackend) existsMetaUnsafe(ctx context.Context, namespace string, key string) (bool, error) {
	nsKey := backend.NamespacedKey(namespace, key)
	_, exists := sb.keys.Get(nsKey)
	return exists, nil
}
