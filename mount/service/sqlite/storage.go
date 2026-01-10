package sqlite

import (
	"context"
	"database/sql"
	"path"
	"time"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

func (s *SqliteObjectStorageService) GetLifecycle() service.Lifecycle {
	return s.driver
}

func (s *SqliteObjectStorageService) CreateObject(ctx context.Context, key string, mode data.FileMode) (data.FileStat, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Check if key already exists
	var exists bool
	err := s.driver.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM vfs_metadata WHERE namespace = '' AND key = ?)", key).Scan(&exists)
	if err != nil {
		return data.FileStat{}, err
	}
	if exists {
		return data.FileStat{}, data.ErrExist
	}

	// Verify parent directory exists (unless this is root)
	parentKey := path.Dir(key)
	if parentKey != "." && parentKey != "" {
		// Check if parent exists and is a directory
		var parentMode data.FileMode
		err := s.driver.db.QueryRowContext(ctx,
			"SELECT mode FROM vfs_metadata WHERE namespace = '' AND key = ?", parentKey).Scan(&parentMode)
		if err == sql.ErrNoRows {
			return data.FileStat{}, data.ErrNotExist
		}
		if err != nil {
			return data.FileStat{}, err
		}
		if !parentMode.IsDir() {
			return data.FileStat{}, data.ErrNotDirectory
		}
	}

	// Create new metadata (generates ID automatically)
	meta := data.NewFileMetadata(key, 0, mode)
	now := time.Now()

	// Insert into database
	_, err = s.driver.db.ExecContext(ctx, `
		INSERT INTO vfs_metadata (id, namespace, key, mode, size, modify_time, access_time, create_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, meta.ID, "", key, int(mode), 0, now.Unix(), now.Unix(), now.Unix())

	if err != nil {
		return data.FileStat{}, err
	}

	// Initialize empty data row with ref_count = 1
	_, err = s.driver.db.ExecContext(ctx, `
		INSERT INTO vfs_data (id, content, size, ref_count, created_at, last_accessed)
		VALUES (?, ?, ?, ?, ?, ?)
	`, meta.ID, []byte{}, 0, 1, now.Unix(), now.Unix())

	if err != nil {
		return data.FileStat{}, err
	}

	// Convert to FileStat
	return data.FileStat{
		Key:         key,
		Mode:        mode,
		Size:        0,
		CreateTime:  now,
		ModifyTime:  now,
		ContentType: "",
	}, nil
}

func (s *SqliteObjectStorageService) ReadObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Query metadata to get ID, mode, and size
	var id string
	var mode data.FileMode
	var size int64
	err := s.driver.db.QueryRowContext(ctx,
		"SELECT id, mode, size FROM vfs_metadata WHERE namespace = '' AND key = ?", key).Scan(&id, &mode, &size)

	if err == sql.ErrNoRows {
		return 0, data.ErrNotExist
	}
	if err != nil {
		return 0, err
	}

	// Check if it's a directory
	if mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	// Check if offset is beyond file size
	if offset >= size {
		return 0, nil // EOF - return 0 bytes read
	}

	// Query data content
	var content []byte
	err = s.driver.db.QueryRowContext(ctx, "SELECT content FROM vfs_data WHERE id = ?", id).Scan(&content)

	if err == sql.ErrNoRows {
		// No data stored yet (empty file)
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Calculate how many bytes to read
	available := size - offset
	toRead := min(int64(len(dat)), available)

	// Copy data from content buffer to output buffer
	copy(dat, content[offset:offset+toRead])

	return int(toRead), nil
}

func (s *SqliteObjectStorageService) WriteObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Query metadata to get ID, mode, and current size
	var id string
	var mode data.FileMode
	var currentSize int64
	err := s.driver.db.QueryRowContext(ctx,
		"SELECT id, mode, size FROM vfs_metadata WHERE namespace = '' AND key = ?", key).Scan(&id, &mode, &currentSize)

	if err == sql.ErrNoRows {
		return 0, data.ErrNotExist
	}
	if err != nil {
		return 0, err
	}

	// Check if it's a directory
	if mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	writeEnd := offset + int64(len(dat))

	// Start transaction for atomic update
	tx, err := s.driver.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Get existing content
	var content []byte
	err = tx.QueryRowContext(ctx,
		"SELECT content FROM vfs_data WHERE id = ?", id).Scan(&content)

	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	// Determine new required size
	newSize := max(writeEnd, currentSize)

	// Expand buffer if needed
	if int64(len(content)) < newSize {
		newBuffer := make([]byte, newSize)
		copy(newBuffer, content)
		content = newBuffer
	}

	// Write the data at offset
	copy(content[offset:], dat)

	now := time.Now().Unix()

	// Update or insert data
	if err == sql.ErrNoRows {
		// Insert new data row
		_, err = tx.ExecContext(ctx, `
			INSERT INTO vfs_data (id, content, size, ref_count, created_at, last_accessed)
			VALUES (?, ?, ?, 1, ?, ?)
		`, id, content, newSize, now, now)
	} else {
		// Update existing data
		_, err = tx.ExecContext(ctx, `
			UPDATE vfs_data SET content = ?, size = ?, last_accessed = ?
			WHERE id = ?
		`, content, newSize, now, id)
	}

	if err != nil {
		return 0, err
	}

	// Update metadata size if file grew
	if writeEnd > currentSize {
		_, err = tx.ExecContext(ctx, `
			UPDATE vfs_metadata SET size = ?, modify_time = ? WHERE id = ?
		`, writeEnd, now, id)

		if err != nil {
			return 0, err
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return len(dat), nil
}

func (s *SqliteObjectStorageService) DeleteObject(ctx context.Context, key string, force bool) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Query metadata to get ID and mode
	var id string
	var mode data.FileMode
	err := s.driver.db.QueryRowContext(ctx,
		"SELECT id, mode FROM vfs_metadata WHERE namespace = '' AND key = ?", key).Scan(&id, &mode)

	if err == sql.ErrNoRows {
		return data.ErrNotExist
	}
	if err != nil {
		return err
	}

	// If it's a directory, handle recursive deletion or validation
	if mode.IsDir() {
		// Collect all children for recursive deletion using SQL query
		pattern := key + "/%"
		rows, err := s.driver.db.QueryContext(ctx,
			"SELECT key FROM vfs_metadata WHERE namespace = '' AND key LIKE ?", pattern)
		if err != nil {
			return err
		}
		defer rows.Close()

		var keysToDelete []string
		for rows.Next() {
			var childKey string
			if err := rows.Scan(&childKey); err != nil {
				return err
			}
			keysToDelete = append(keysToDelete, childKey)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// Delete all children first (recursive)
		for _, childKey := range keysToDelete {
			if err := s.deleteObjectInternal(ctx, childKey); err != nil {
				// Continue deleting even if some fail
				// Could collect errors and return combined error
			}
		}
	}

	// Delete the object itself
	return s.deleteObjectInternal(ctx, key)
}

// deleteObjectInternal deletes a single object without recursive checks
// MUST be called while holding a write lock
func (s *SqliteObjectStorageService) deleteObjectInternal(ctx context.Context, key string) error {
	// Query metadata to get ID
	var id string
	err := s.driver.db.QueryRowContext(ctx,
		"SELECT id FROM vfs_metadata WHERE namespace = '' AND key = ?", key).Scan(&id)
	if err == sql.ErrNoRows {
		return data.ErrNotExist
	}
	if err != nil {
		return err
	}

	// Start transaction for atomic deletion
	tx, err := s.driver.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete metadata
	_, err = tx.ExecContext(ctx, "DELETE FROM vfs_metadata WHERE id = ?", id)
	if err != nil {
		return err
	}

	// Decrement ref_count for associated data
	_, err = tx.ExecContext(ctx, `
		UPDATE vfs_data
		SET ref_count = ref_count - 1
		WHERE id = ?
	`, id)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Delete data rows with ref_count = 0 (cleanup unreferenced data)
	_, err = tx.ExecContext(ctx, "DELETE FROM vfs_data WHERE ref_count = 0")
	if err != nil {
		return err
	}

	// Commit transaction
	return tx.Commit()
}

func (s *SqliteObjectStorageService) ListObjects(ctx context.Context, key string) ([]data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// For root directory (key == ""), skip existence check - root is implicit
	if key != "" {
		// Query metadata to check if it's a file or directory
		var mode data.FileMode
		var size int64
		var contentType sql.NullString
		var modifyTime, createTime int64

		err := s.driver.db.QueryRowContext(ctx, `
			SELECT mode, size, modify_time, create_time, content_type
			FROM vfs_metadata WHERE namespace = '' AND key = ?
		`, key).Scan(&mode, &size, &modifyTime, &createTime, &contentType)

		if err == sql.ErrNoRows {
			return nil, data.ErrNotExist
		}
		if err != nil {
			return nil, err
		}

		// For files, return single entry
		if !mode.IsDir() {
			stat := data.FileStat{
				Key:        key,
				Mode:       mode,
				Size:       size,
				CreateTime: time.Unix(createTime, 0),
				ModifyTime: time.Unix(modifyTime, 0),
			}
			if contentType.Valid {
				stat.ContentType = data.ContentType(contentType.String)
			}
			return []data.FileStat{stat}, nil
		}
	}

	// For directories, use SQL query to find direct children
	var rows *sql.Rows
	var err error

	if key == "" {
		// Root directory: find all entries without a slash
		rows, err = s.driver.db.QueryContext(ctx, `
			SELECT key, mode, size, modify_time, create_time, content_type
			FROM vfs_metadata
			WHERE namespace = '' AND key NOT LIKE '%/%'
		`)
	} else {
		// Subdirectory: find direct children (entries starting with key/ but not containing another /)
		pattern1 := key + "/%"
		pattern2 := key + "/%/%"
		rows, err = s.driver.db.QueryContext(ctx, `
			SELECT key, mode, size, modify_time, create_time, content_type
			FROM vfs_metadata
			WHERE namespace = '' AND key LIKE ? AND key NOT LIKE ?
		`, pattern1, pattern2)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Process query results
	var result []data.FileStat
	for rows.Next() {
		var stat data.FileStat
		var contentType sql.NullString
		var modifyTime, createTime int64

		err := rows.Scan(&stat.Key, &stat.Mode, &stat.Size, &modifyTime, &createTime, &contentType)
		if err != nil {
			return nil, err
		}

		stat.CreateTime = time.Unix(createTime, 0)
		stat.ModifyTime = time.Unix(modifyTime, 0)
		if contentType.Valid {
			stat.ContentType = data.ContentType(contentType.String)
		}
		result = append(result, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *SqliteObjectStorageService) HeadObject(ctx context.Context, key string) (data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Query metadata from database
	var stat data.FileStat
	var contentType sql.NullString
	var modifyTime, createTime int64

	err := s.driver.db.QueryRowContext(ctx, `
		SELECT key, mode, size, modify_time, create_time, content_type
		FROM vfs_metadata WHERE namespace = '' AND key = ?
	`, key).Scan(&stat.Key, &stat.Mode, &stat.Size, &modifyTime, &createTime, &contentType)

	if err == sql.ErrNoRows {
		return data.FileStat{}, data.ErrNotExist
	}
	if err != nil {
		return data.FileStat{}, err
	}

	// Convert timestamps
	stat.ModifyTime = time.Unix(modifyTime, 0)
	stat.CreateTime = time.Unix(createTime, 0)

	// Convert content type
	if contentType.Valid {
		stat.ContentType = data.ContentType(contentType.String)
	}

	return stat, nil
}

func (s *SqliteObjectStorageService) TruncateObject(ctx context.Context, key string, size int64) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Query metadata to get ID, mode, and current size
	var id string
	var mode data.FileMode
	var currentSize int64
	err := s.driver.db.QueryRowContext(ctx,
		"SELECT id, mode, size FROM vfs_metadata WHERE namespace = '' AND key = ?", key).Scan(&id, &mode, &currentSize)

	if err == sql.ErrNoRows {
		return data.ErrNotExist
	}
	if err != nil {
		return err
	}

	// Check if it's a directory
	if mode.IsDir() {
		return data.ErrIsDirectory
	}

	// No changes needed if size is the same
	if size == currentSize {
		return nil
	}

	// Start transaction for atomic update
	tx, err := s.driver.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get existing content
	var content []byte
	err = tx.QueryRowContext(ctx,
		"SELECT content FROM vfs_data WHERE id = ?", id).Scan(&content)

	now := time.Now().Unix()

	if err == sql.ErrNoRows {
		// No existing data - create new empty content of specified size
		content = make([]byte, size)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO vfs_data (id, content, size, ref_count, created_at, last_accessed)
			VALUES (?, ?, ?, 1, ?, ?)
		`, id, content, size, now, now)
	} else if err != nil {
		return err
	} else {
		// Adjust content size
		if size < int64(len(content)) {
			// Shrink - truncate content
			content = content[:size]
		} else {
			// Expand - pad with zeros
			newData := make([]byte, size)
			copy(newData, content)
			content = newData
		}

		// Update existing data
		_, err = tx.ExecContext(ctx, `
			UPDATE vfs_data SET content = ?, size = ?, last_accessed = ?
			WHERE id = ?
		`, content, size, now, id)
	}

	if err != nil {
		return err
	}

	// Update metadata size
	_, err = tx.ExecContext(ctx, `
		UPDATE vfs_metadata SET size = ?, modify_time = ? WHERE id = ?
	`, size, now, id)

	if err != nil {
		return err
	}

	// Commit transaction
	return tx.Commit()
}
