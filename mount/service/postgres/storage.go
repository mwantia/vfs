package postgres

import (
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

// GetLifecycle returns the driver's lifecycle interface
func (s *PostgresObjectStorageService) GetLifecycle() service.Lifecycle {
	return s.driver
}

// CreateObject creates a new file or directory in the storage
func (s *PostgresObjectStorageService) CreateObject(ctx context.Context, key string, mode data.FileMode) (data.FileStat, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	// Acquire connection
	conn, err := s.driver.pool.Acquire(ctx)
	if err != nil {
		return data.FileStat{}, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Check if key already exists
	var exists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM vfs_metadata WHERE namespace = '' AND key = $1)", key).Scan(&exists)
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
		err := conn.QueryRow(ctx,
			"SELECT mode FROM vfs_metadata WHERE namespace = '' AND key = $1", parentKey).Scan(&parentMode)
		if err == pgx.ErrNoRows {
			return data.FileStat{}, data.ErrNotExist
		}
		if err != nil {
			return data.FileStat{}, err
		}
		if !parentMode.IsDir() {
			return data.FileStat{}, data.ErrNotDirectory
		}
	}

	// Create new metadata
	meta := data.NewFileMetadata(key, 0, mode)
	now := time.Now()

	// Insert into database
	_, err = conn.Exec(ctx, `
		INSERT INTO vfs_metadata (id, namespace, key, mode, size, modify_time, access_time, create_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, meta.ID, "", key, int(mode), 0, now.Unix(), now.Unix(), now.Unix())

	if err != nil {
		return data.FileStat{}, fmt.Errorf("failed to insert metadata: %w", err)
	}

	return data.FileStat{
		Key:        key,
		Mode:       mode,
		Size:       0,
		CreateTime: now,
		ModifyTime: now,
	}, nil
}

// ReadObject reads data from an object at the specified offset
func (s *PostgresObjectStorageService) ReadObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	conn, err := s.driver.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Query metadata to get ID, mode, and size
	var id string
	var mode data.FileMode
	var size int64
	err = conn.QueryRow(ctx,
		"SELECT id, mode, size FROM vfs_metadata WHERE namespace = '' AND key = $1", key).Scan(&id, &mode, &size)

	if err == pgx.ErrNoRows {
		return 0, data.ErrNotExist
	}
	if err != nil {
		return 0, fmt.Errorf("failed to query metadata: %w", err)
	}

	if mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	if offset >= size {
		return 0, io.EOF
	}

	// Query data from database
	var content []byte
	err = conn.QueryRow(ctx,
		"SELECT content FROM vfs_data WHERE id = $1", id).Scan(&content)

	if err == pgx.ErrNoRows {
		// No data stored yet (empty file)
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to query data: %w", err)
	}

	// Calculate how many bytes we can actually read
	available := size - offset
	toRead := min(int64(len(dat)), available)

	// Copy data from content
	n := copy(dat, content[offset:offset+toRead])
	return n, nil
}

// WriteObject writes data to an object at the specified offset
func (s *PostgresObjectStorageService) WriteObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	conn, err := s.driver.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Query metadata to get ID, mode, and current size
	var id string
	var mode data.FileMode
	var currentSize int64
	err = conn.QueryRow(ctx,
		"SELECT id, mode, size FROM vfs_metadata WHERE namespace = '' AND key = $1", key).Scan(&id, &mode, &currentSize)

	if err == pgx.ErrNoRows {
		return 0, data.ErrNotExist
	}
	if err != nil {
		return 0, fmt.Errorf("failed to query metadata: %w", err)
	}

	if mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	writeEnd := offset + int64(len(dat))

	// Start transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get existing content or create new
	var content []byte
	var refCount int
	err = tx.QueryRow(ctx,
		"SELECT content, ref_count FROM vfs_data WHERE id = $1", id).Scan(&content, &refCount)

	if err != nil && err != pgx.ErrNoRows {
		return 0, fmt.Errorf("failed to query existing data: %w", err)
	}

	// Determine the new required size
	newSize := max(writeEnd, currentSize)

	// Expand buffer if needed
	if int64(len(content)) < newSize {
		newBuffer := make([]byte, newSize)
		copy(newBuffer, content)
		content = newBuffer
	}

	// Write the data
	copy(content[offset:], dat)

	now := time.Now().Unix()

	// Insert or update content
	if err == pgx.ErrNoRows {
		// Insert new data
		_, err = tx.Exec(ctx, `
			INSERT INTO vfs_data (id, content, size, ref_count, created_at, last_accessed)
			VALUES ($1, $2, $3, 1, $4, $5)
		`, id, content, newSize, now, now)
	} else {
		// Update existing data
		_, err = tx.Exec(ctx, `
			UPDATE vfs_data SET content = $1, size = $2, last_accessed = $3
			WHERE id = $4
		`, content, newSize, now, id)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to write data: %w", err)
	}

	// Update metadata size if file grew
	if writeEnd > currentSize {
		_, err = tx.Exec(ctx, `
			UPDATE vfs_metadata SET size = $1, modify_time = $2 WHERE id = $3
		`, writeEnd, now, id)

		if err != nil {
			return 0, fmt.Errorf("failed to update metadata: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return len(dat), nil
}

// DeleteObject deletes an object (file or directory)
func (s *PostgresObjectStorageService) DeleteObject(ctx context.Context, key string, force bool) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	conn, err := s.driver.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Query metadata to get ID and mode
	var id string
	var mode data.FileMode
	err = conn.QueryRow(ctx,
		"SELECT id, mode FROM vfs_metadata WHERE namespace = '' AND key = $1", key).Scan(&id, &mode)

	if err == pgx.ErrNoRows {
		return data.ErrNotExist
	}
	if err != nil {
		return fmt.Errorf("failed to query metadata: %w", err)
	}

	// If it's a directory, handle recursive deletion
	if mode.IsDir() {
		// Directories ALWAYS require force=true (even if empty)
		if !force {
			return data.ErrIsDirectory
		}

		// Collect all children for recursive deletion using SQL query
		pattern := key + "/%"
		rows, err := conn.Query(ctx,
			"SELECT id FROM vfs_metadata WHERE namespace = '' AND key LIKE $1", pattern)
		if err != nil {
			return fmt.Errorf("failed to query children: %w", err)
		}
		defer rows.Close()

		var idsToDelete []string
		idsToDelete = append(idsToDelete, id) // Include the directory itself

		for rows.Next() {
			var childID string
			if err := rows.Scan(&childID); err != nil {
				return fmt.Errorf("failed to scan child: %w", err)
			}
			idsToDelete = append(idsToDelete, childID)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to iterate children: %w", err)
		}

		// Delete all collected IDs in transaction
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		defer tx.Rollback(ctx)

		for _, delID := range idsToDelete {
			// Delete metadata
			_, err = tx.Exec(ctx, "DELETE FROM vfs_metadata WHERE id = $1", delID)
			if err != nil {
				return fmt.Errorf("failed to delete metadata: %w", err)
			}

			// Decrement ref count
			_, err = tx.Exec(ctx, `
				UPDATE vfs_data SET ref_count = ref_count - 1 WHERE id = $1
			`, delID)
			if err != nil && err != pgx.ErrNoRows {
				return fmt.Errorf("failed to update ref count: %w", err)
			}
		}

		// Delete data with ref_count <= 0
		_, err = tx.Exec(ctx, "DELETE FROM vfs_data WHERE ref_count <= 0")
		if err != nil {
			return fmt.Errorf("failed to cleanup data: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		return nil
	}

	// For files, delete directly
	return s.deleteObjectInternal(ctx, key, id)
}

// deleteObjectInternal deletes a single object without recursive checks
func (s *PostgresObjectStorageService) deleteObjectInternal(ctx context.Context, key, id string) error {
	conn, err := s.driver.pool.Acquire(ctx)
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

	// Decrement ref_count for associated data
	_, err = tx.Exec(ctx, `
		UPDATE vfs_data SET ref_count = ref_count - 1 WHERE id = $1
	`, id)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to update ref count: %w", err)
	}

	// Delete data rows with ref_count <= 0 (cleanup unreferenced data)
	_, err = tx.Exec(ctx, "DELETE FROM vfs_data WHERE ref_count <= 0")
	if err != nil {
		return fmt.Errorf("failed to cleanup data: %w", err)
	}

	// Commit transaction
	return tx.Commit(ctx)
}

// ListObjects lists direct children of a directory or returns info for a file
func (s *PostgresObjectStorageService) ListObjects(ctx context.Context, key string) ([]data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// For root directory, skip the existence check - root is implicit
	if key != "" {
		conn, err := s.driver.pool.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire connection: %w", err)
		}
		defer conn.Release()

		// Query metadata to check if it's a file or directory
		var mode data.FileMode
		var size int64
		var modifyTime, createTime int64
		var contentType *string

		err = conn.QueryRow(ctx, `
			SELECT mode, size, modify_time, create_time, content_type
			FROM vfs_metadata WHERE namespace = '' AND key = $1
		`, key).Scan(&mode, &size, &modifyTime, &createTime, &contentType)

		if err == pgx.ErrNoRows {
			return nil, data.ErrNotExist
		}
		if err != nil {
			return nil, fmt.Errorf("failed to query metadata: %w", err)
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
			if contentType != nil {
				stat.ContentType = data.ContentType(*contentType)
			}
			return []data.FileStat{stat}, nil
		}
	}

	// For directories, use SQL query to find direct children
	conn, err := s.driver.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var rows pgx.Rows
	if key == "" {
		// Root directory: find all entries without a slash
		rows, err = conn.Query(ctx, `
			SELECT key, mode, size, modify_time, create_time, content_type
			FROM vfs_metadata
			WHERE namespace = '' AND key NOT LIKE '%/%'
		`)
	} else {
		// Subdirectory: find direct children (entries starting with key/ but not containing another /)
		pattern1 := key + "/%"
		pattern2 := key + "/%/%"
		rows, err = conn.Query(ctx, `
			SELECT key, mode, size, modify_time, create_time, content_type
			FROM vfs_metadata
			WHERE namespace = '' AND key LIKE $1 AND key NOT LIKE $2
		`, pattern1, pattern2)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query children: %w", err)
	}
	defer rows.Close()

	// Process query results
	var result []data.FileStat
	for rows.Next() {
		var stat data.FileStat
		var modifyTime, createTime int64
		var contentType *string

		err := rows.Scan(&stat.Key, &stat.Mode, &stat.Size, &modifyTime, &createTime, &contentType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		stat.CreateTime = time.Unix(createTime, 0)
		stat.ModifyTime = time.Unix(modifyTime, 0)
		if contentType != nil {
			stat.ContentType = data.ContentType(*contentType)
		}
		result = append(result, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate rows: %w", err)
	}

	return result, nil
}

// HeadObject returns metadata for an object without reading its content
func (s *PostgresObjectStorageService) HeadObject(ctx context.Context, key string) (data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	conn, err := s.driver.pool.Acquire(ctx)
	if err != nil {
		return data.FileStat{}, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Query metadata from database
	var stat data.FileStat
	var modifyTime, createTime int64
	var contentType *string

	err = conn.QueryRow(ctx, `
		SELECT key, mode, size, modify_time, create_time, content_type
		FROM vfs_metadata WHERE namespace = '' AND key = $1
	`, key).Scan(&stat.Key, &stat.Mode, &stat.Size, &modifyTime, &createTime, &contentType)

	if err == pgx.ErrNoRows {
		return data.FileStat{}, data.ErrNotExist
	}
	if err != nil {
		return data.FileStat{}, fmt.Errorf("failed to query metadata: %w", err)
	}

	// Convert timestamps
	stat.ModifyTime = time.Unix(modifyTime, 0)
	stat.CreateTime = time.Unix(createTime, 0)

	// Convert content type
	if contentType != nil {
		stat.ContentType = data.ContentType(*contentType)
	}

	return stat, nil
}

// TruncateObject resizes an object to the specified size
func (s *PostgresObjectStorageService) TruncateObject(ctx context.Context, key string, size int64) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	conn, err := s.driver.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Query metadata to get ID, mode, and current size
	var id string
	var mode data.FileMode
	var currentSize int64
	err = conn.QueryRow(ctx,
		"SELECT id, mode, size FROM vfs_metadata WHERE namespace = '' AND key = $1", key).Scan(&id, &mode, &currentSize)

	if err == pgx.ErrNoRows {
		return data.ErrNotExist
	}
	if err != nil {
		return fmt.Errorf("failed to query metadata: %w", err)
	}

	if mode.IsDir() {
		return data.ErrIsDirectory
	}

	if size == currentSize {
		return nil // No changes needed
	}

	// Start transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get existing content
	var content []byte
	err = tx.QueryRow(ctx,
		"SELECT content FROM vfs_data WHERE id = $1", id).Scan(&content)

	now := time.Now().Unix()

	if err == pgx.ErrNoRows {
		// No existing data - create new empty content of specified size
		content = make([]byte, size)
		_, err = tx.Exec(ctx, `
			INSERT INTO vfs_data (id, content, size, ref_count, created_at, last_accessed)
			VALUES ($1, $2, $3, 1, $4, $5)
		`, id, content, size, now, now)
	} else if err != nil {
		return fmt.Errorf("failed to query existing data: %w", err)
	} else {
		// Adjust content size
		if size < int64(len(content)) {
			content = content[:size]
		} else {
			newData := make([]byte, size)
			copy(newData, content)
			content = newData
		}

		// Update existing data
		_, err = tx.Exec(ctx, `
			UPDATE vfs_data SET content = $1, size = $2, last_accessed = $3
			WHERE id = $4
		`, content, size, now, id)
	}

	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Update metadata size in transaction
	_, err = tx.Exec(ctx, `
		UPDATE vfs_metadata SET size = $1, modify_time = $2 WHERE id = $3
	`, size, now, id)

	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
