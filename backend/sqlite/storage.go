package sqlite

import (
	"context"
	"database/sql"
	"io"
	"path"
	"strings"
	"time"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
)

func (sb *SQLiteBackend) CreateObject(ctx context.Context, key string, mode data.VirtualFileMode) (*data.VirtualFileStat, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Check if path exists in B-tree
	if _, exists := sb.keys.Get(key); exists {
		return nil, data.ErrExist
	}

	// Verify parent directory exists
	parentKey := path.Dir(key)
	if parentKey != "." && parentKey != "" {
		parentMeta, err := sb.ReadMeta(ctx, parentKey)
		if err != nil {
			return nil, data.ErrNotExist
		}

		if !parentMeta.Mode.IsDir() {
			return nil, data.ErrNotDirectory
		}
	}

	meta := data.NewFileMetadata(key, 0, mode)
	stat := meta.ToStat()

	return stat, sb.CreateMeta(ctx, meta)
}

func (sb *SQLiteBackend) ReadObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	meta, err := sb.ReadMeta(ctx, key)
	if err != nil {
		return 0, err
	}

	if meta.Mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	if offset >= meta.Size {
		return 0, io.EOF
	}

	// Query data from database
	var content []byte
	err = sb.db.QueryRowContext(ctx,
		"SELECT content FROM vfs_data WHERE id = ?",
		meta.ID).Scan(&content)

	if err == sql.ErrNoRows {
		// No data stored yet (empty file)
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Calculate how many bytes we can actually read
	available := meta.Size - offset
	toRead := min(int64(len(dat)), available)

	// Copy data from content
	n := copy(dat, content[offset:offset+toRead])
	return n, nil
}

func (sb *SQLiteBackend) WriteObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	meta, err := sb.ReadMeta(ctx, key)
	if err != nil {
		return 0, err
	}

	if meta.Mode.IsDir() {
		return 0, data.ErrIsDirectory
	}

	writeEnd := offset + int64(len(dat))

	// Start transaction
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Get existing content or create new
	var content []byte
	var refCount int
	err = tx.QueryRowContext(ctx,
		"SELECT content, ref_count FROM vfs_data WHERE id = ?",
		meta.ID).Scan(&content, &refCount)

	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	// Determine the new required size
	newSize := max(writeEnd, meta.Size)

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
	if err == sql.ErrNoRows {
		// Insert new data
		_, err = tx.ExecContext(ctx, `
			INSERT INTO vfs_data (id, content, size, ref_count, created_at, last_accessed)
			VALUES (?, ?, ?, 1, ?, ?)
		`, meta.ID, content, newSize, now, now)
	} else {
		// Update existing data
		_, err = tx.ExecContext(ctx, `
			UPDATE vfs_data SET content = ?, size = ?, last_accessed = ?
			WHERE id = ?
		`, content, newSize, now, meta.ID)
	}

	if err != nil {
		return 0, err
	}

	// Update metadata size in transaction
	if writeEnd > meta.Size {
		meta.Size = writeEnd
		meta.ModifyTime = time.Now()

		// Update in the same transaction
		_, err = tx.ExecContext(ctx, `
			UPDATE vfs_metadata SET size = ?, modify_time = ? WHERE id = ?
		`, meta.Size, meta.ModifyTime.Unix(), meta.ID)

		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return len(dat), nil
}

func (sb *SQLiteBackend) DeleteObject(ctx context.Context, key string, force bool) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	meta, err := sb.ReadMeta(ctx, key)
	if err != nil {
		return err
	}

	if meta.Mode.IsDir() {
		// Directories can only be deleted with force=true
		if !force {
			return data.ErrIsDirectory
		}

		// Build prefix for children lookup
		prefixKey := key
		if prefixKey != "" {
			prefixKey += "/"
		}

		// Collect all paths to delete (including this directory)
		var keysToDelete []string
		keysToDelete = append(keysToDelete, key)

		// Use B-tree range scan to find all children
		sb.keys.Scan(func(childPath string, _ string) bool {
			if strings.HasPrefix(childPath, prefixKey) {
				keysToDelete = append(keysToDelete, childPath)
			}
			// Continue scanning
			return true
		})

		errs := errors.Errors{}

		// Delete all collected paths
		for _, delKey := range keysToDelete {
			if err := sb.DeleteMeta(ctx, delKey); err != nil {
				errs.Add(err)
			}
		}

		return errs.Errors()
	}

	return sb.DeleteMeta(ctx, key)
}

func (sb *SQLiteBackend) ListObjects(ctx context.Context, key string) ([]*data.VirtualFileStat, error) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	// For root directory, skip the existence check - root is implicit
	if key != "" {
		meta, err := sb.ReadMeta(ctx, key)
		if err != nil {
			return nil, err
		}

		// For files, return single entry
		if !meta.Mode.IsDir() {
			return []*data.VirtualFileStat{
				meta.ToStat(),
			}, nil
		}
	}

	// For directories, use B-tree range scan to find children
	prefixKey := key
	if prefixKey != "" {
		prefixKey += "/"
	}
	prefixLen := len(prefixKey)

	// Use map to deduplicate direct children
	children := make(map[string]*data.VirtualFileMetadata)

	// B-tree range scan: iterate over all paths starting with prefix
	sb.keys.Scan(func(childPath string, childID string) bool {
		// Skip the directory itself
		if childPath == key {
			return true
		}

		// Check if this path is under our directory
		if !strings.HasPrefix(childPath, prefixKey) {
			return true // Continue scanning (paths are ordered)
		}

		// Get relative path
		rel := childPath[prefixLen:]

		// Skip empty relative paths (shouldn't happen but be safe)
		if rel == "" {
			return true
		}

		// Check if this is a direct child (no slash in relative path)
		if slashIdx := strings.IndexByte(rel, '/'); slashIdx > 0 {
			// This is a nested child - only track the first segment
			childName := rel[:slashIdx]
			if _, seen := children[childName]; !seen {
				// Look up the directory metadata
				dirPath := prefixKey + childName
				dirMeta, err := sb.ReadMeta(ctx, dirPath)
				if err == nil {
					children[childName] = dirMeta
				}
			}
		} else {
			childMeta, err := sb.ReadMeta(ctx, childPath)
			if err == nil {
				children[rel] = childMeta
			}
		}

		// Continue scanning
		return true
	})

	// Convert map to slice
	result := make([]*data.VirtualFileStat, 0, len(children))
	for _, childMeta := range children {
		stat := childMeta.ToStat()
		result = append(result, stat)
	}

	return result, nil
}

func (sb *SQLiteBackend) HeadObject(ctx context.Context, key string) (*data.VirtualFileStat, error) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	meta, err := sb.ReadMeta(ctx, key)
	if err != nil {
		return nil, err
	}

	return meta.ToStat(), nil
}

func (sb *SQLiteBackend) TruncateObject(ctx context.Context, key string, size int64) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	meta, err := sb.ReadMeta(ctx, key)
	if err != nil {
		return err
	}

	if meta.Mode.IsDir() {
		return data.ErrIsDirectory
	}

	if size == meta.Size {
		return nil // No changes needed
	}

	// Start transaction
	tx, err := sb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get existing content
	var content []byte
	err = tx.QueryRowContext(ctx,
		"SELECT content FROM vfs_data WHERE id = ?",
		meta.ID).Scan(&content)

	now := time.Now().Unix()

	if err == sql.ErrNoRows {
		// No existing data - create new empty content of specified size
		content = make([]byte, size)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO vfs_data (id, content, size, ref_count, created_at, last_accessed)
			VALUES (?, ?, ?, 1, ?, ?)
		`, meta.ID, content, size, now, now)
	} else if err != nil {
		return err
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
		_, err = tx.ExecContext(ctx, `
			UPDATE vfs_data SET content = ?, size = ?, last_accessed = ?
			WHERE id = ?
		`, content, size, now, meta.ID)
	}

	if err != nil {
		return err
	}

	// Update metadata size in transaction
	meta.Size = size
	meta.ModifyTime = time.Now()

	_, err = tx.ExecContext(ctx, `
		UPDATE vfs_metadata SET size = ?, modify_time = ? WHERE id = ?
	`, meta.Size, meta.ModifyTime.Unix(), meta.ID)

	if err != nil {
		return err
	}

	return tx.Commit()
}
