package mounts

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver (CGO_ENABLED=0 compatible)

	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/data"
)

// SQLiteMount provides a virtual filesystem backed by a SQLite database.
// All files and directories are stored in database tables with full CRUD support.
// This implementation uses modernc.org/sqlite which works without CGO.
type SQLiteMount struct {
	mu sync.RWMutex
	db *sql.DB
}

// NewSQLite creates a new SQLite-backed virtual mount.
// The dbPath can be ":memory:" for an in-memory database or a file path.
func NewSQLite(dbPath string) (*SQLiteMount, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	mount := &SQLiteMount{
		db: db,
	}

	if err := mount.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return mount, nil
}

// initSchema creates the necessary tables for storing files and directories.
func (sm *SQLiteMount) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS vfs_files (
		path TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		is_dir INTEGER NOT NULL,
		size INTEGER NOT NULL DEFAULT 0,
		mode INTEGER NOT NULL DEFAULT 420,
		mod_time INTEGER NOT NULL,
		data BLOB,
		metadata TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_parent ON vfs_files(path);
	CREATE INDEX IF NOT EXISTS idx_is_dir ON vfs_files(is_dir);
	`

	_, err := sm.db.Exec(schema)
	if err != nil {
		return err
	}

	// Create root directory if it doesn't exist
	_, err = sm.db.Exec(`
		INSERT OR IGNORE INTO vfs_files (path, name, is_dir, size, mode, mod_time)
		VALUES ('', '', 1, 0, ?, ?)
	`, int(data.ModeDir|0755), time.Now().Unix())

	return err
}

// GetCapabilities returns the capabilities supported by this mount.
func (sm *SQLiteMount) GetCapabilities() vfs.VirtualMountCapabilities {
	return vfs.VirtualMountCapabilities{
		Capabilities: []vfs.VirtualMountCapability{
			vfs.VirtualMountCapabilityCRUD,
			vfs.VirtualMountCapabilityMetadata,
			vfs.VirtualMountCapabilityQuery,
		},
	}
}

// Mount is called when the mount is being attached to the VFS.
// For SQLiteMount, this verifies the database connection is healthy.
func (sm *SQLiteMount) Mount(ctx context.Context, path string, vfsInstance *vfs.VirtualFileSystem) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Verify database connection is alive
	if err := sm.db.PingContext(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}

	return nil
}

// Unmount is called when the mount is being detached from the VFS.
// For SQLiteMount, this ensures all pending transactions are committed and the database is closed safely.
func (sm *SQLiteMount) Unmount(ctx context.Context, path string, vfsInstance *vfs.VirtualFileSystem) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Close the database connection
	// This will flush any pending writes and close the connection safely
	if err := sm.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	return nil
}

// Create creates a new file or directory in the database.
func (sm *SQLiteMount) Create(ctx context.Context, p string, isDir bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if already exists
	var exists int
	err := sm.db.QueryRowContext(ctx, "SELECT 1 FROM vfs_files WHERE path = ?", p).Scan(&exists)
	if err == nil {
		return vfs.ErrExist
	}
	if err != sql.ErrNoRows {
		return err
	}

	// Verify parent directory exists
	parent := path.Dir(p)
	if parent != "." && parent != "" {
		var isParentDir int
		err = sm.db.QueryRowContext(ctx, "SELECT is_dir FROM vfs_files WHERE path = ?", parent).Scan(&isParentDir)
		if err == sql.ErrNoRows {
			return vfs.ErrNotExist
		}
		if err != nil {
			return err
		}
		if isParentDir == 0 {
			return vfs.ErrNotDirectory
		}
	}

	mode := 0644
	if isDir {
		mode = int(data.ModeDir | 0755)
	}

	_, err = sm.db.ExecContext(ctx, `
		INSERT INTO vfs_files (path, name, is_dir, size, mode, mod_time, data)
		VALUES (?, ?, ?, 0, ?, ?, ?)
	`, p, path.Base(p), boolToInt(isDir), mode, time.Now().Unix(), []byte{})

	return err
}

// Read reads data from a file at the given offset.
func (sm *SQLiteMount) Read(ctx context.Context, p string, offset int64, data []byte) (int, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var isDir int
	var fileData []byte
	var size int64

	err := sm.db.QueryRowContext(ctx, `
		SELECT is_dir, data, size FROM vfs_files WHERE path = ?
	`, p).Scan(&isDir, &fileData, &size)

	if err == sql.ErrNoRows {
		return 0, vfs.ErrNotExist
	}
	if err != nil {
		return 0, err
	}

	if isDir == 1 {
		return 0, vfs.ErrIsDirectory
	}

	if offset >= size {
		return 0, io.EOF
	}

	available := size - offset
	toRead := int64(len(data))
	if toRead > available {
		toRead = available
	}

	n := copy(data, fileData[offset:offset+toRead])
	return n, nil
}

// Write writes data to a file at the given offset.
func (sm *SQLiteMount) Write(ctx context.Context, p string, offset int64, data []byte) (int, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var isDir int
	var fileData []byte
	var size int64

	err := sm.db.QueryRowContext(ctx, `
		SELECT is_dir, data, size FROM vfs_files WHERE path = ?
	`, p).Scan(&isDir, &fileData, &size)

	if err == sql.ErrNoRows {
		return 0, vfs.ErrNotExist
	}
	if err != nil {
		return 0, err
	}

	if isDir == 1 {
		return 0, vfs.ErrIsDirectory
	}

	// Calculate new size if writing beyond current end
	writeEnd := offset + int64(len(data))
	if writeEnd > size {
		newData := make([]byte, writeEnd)
		copy(newData, fileData)
		copy(newData[offset:], data)
		fileData = newData
		size = writeEnd
	} else {
		// Ensure fileData is large enough
		if int64(len(fileData)) < writeEnd {
			newData := make([]byte, writeEnd)
			copy(newData, fileData)
			fileData = newData
		}
		copy(fileData[offset:], data)
	}

	_, err = sm.db.ExecContext(ctx, `
		UPDATE vfs_files SET data = ?, size = ?, mod_time = ? WHERE path = ?
	`, fileData, size, time.Now().Unix(), p)

	if err != nil {
		return 0, err
	}

	return len(data), nil
}

// Delete removes a file or directory from the database.
func (sm *SQLiteMount) Delete(ctx context.Context, p string, force bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var isDir int
	err := sm.db.QueryRowContext(ctx, "SELECT is_dir FROM vfs_files WHERE path = ?", p).Scan(&isDir)
	if err == sql.ErrNoRows {
		return vfs.ErrNotExist
	}
	if err != nil {
		return err
	}

	// Directories require force=true to delete (proper filesystem semantics)
	if isDir == 1 && !force {
		return vfs.ErrIsDirectory
	}

	if force && isDir == 1 {
		// Delete all children recursively
		prefix := p
		if prefix != "" {
			prefix += "/"
		}
		_, err = sm.db.ExecContext(ctx, `
			DELETE FROM vfs_files WHERE path = ? OR path LIKE ? || '%'
		`, p, prefix)
	} else {
		_, err = sm.db.ExecContext(ctx, "DELETE FROM vfs_files WHERE path = ?", p)
	}

	return err
}

// List returns all objects under the given path.
func (sm *SQLiteMount) List(ctx context.Context, p string) ([]*data.VirtualFileInfo, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var isDir int
	err := sm.db.QueryRowContext(ctx, "SELECT is_dir FROM vfs_files WHERE path = ?", p).Scan(&isDir)
	if err == sql.ErrNoRows {
		return nil, vfs.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	// For files, return single entry
	if isDir == 0 {
		info, err := sm.statInternal(ctx, p)
		if err != nil {
			return nil, err
		}
		return []*data.VirtualFileInfo{info}, nil
	}

	// For directories, return direct children
	prefix := p
	if prefix != "" {
		prefix += "/"
	}

	rows, err := sm.db.QueryContext(ctx, `
		SELECT path, name, is_dir, size, mode, mod_time 
		FROM vfs_files 
		WHERE path != ? AND path LIKE ? || '%'
	`, p, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	seen := make(map[string]bool)
	var objects []*data.VirtualFileInfo

	for rows.Next() {
		var objPath, name string
		var isDir, mode int
		var size, modTime int64

		if err := rows.Scan(&objPath, &name, &isDir, &size, &mode, &modTime); err != nil {
			continue
		}

		rel := strings.TrimPrefix(objPath, prefix)
		parts := strings.Split(rel, "/")
		if len(parts) == 0 {
			continue
		}

		childName := parts[0]
		if seen[childName] {
			continue
		}
		seen[childName] = true

		childPath := prefix + childName
		isChildDir := isDir == 1
		if len(parts) > 1 {
			isChildDir = true
		}

		objType := data.NodeTypeFile
		objMode := data.VirtualFileMode(mode)
		objSize := size

		if isChildDir {
			objType = data.NodeTypeDirectory
			objMode = data.ModeDir | 0755
			objSize = 0
		}

		objects = append(objects, &data.VirtualFileInfo{
			Path:       childPath,
			Type:       objType,
			Size:       objSize,
			Mode:       objMode,
			ModifyTime: time.Unix(modTime, 0),
		})
	}

	return objects, rows.Err()
}

// Stat returns information about a file or directory.
func (sm *SQLiteMount) Stat(ctx context.Context, p string) (*data.VirtualFileInfo, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.statInternal(ctx, p)
}

// statInternal is the internal implementation of Stat without locking.
func (sm *SQLiteMount) statInternal(ctx context.Context, p string) (*data.VirtualFileInfo, error) {
	var name string
	var isDir, mode int
	var size, modTime int64

	err := sm.db.QueryRowContext(ctx, `
		SELECT name, is_dir, size, mode, mod_time 
		FROM vfs_files WHERE path = ?
	`, p).Scan(&name, &isDir, &size, &mode, &modTime)

	if err == sql.ErrNoRows {
		return nil, vfs.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	objType := data.NodeTypeFile
	if isDir == 1 {
		objType = data.NodeTypeDirectory
	}

	return &data.VirtualFileInfo{
		Path:       p,
		Type:       objType,
		Size:       size,
		Mode:       data.VirtualFileMode(mode),
		ModifyTime: time.Unix(modTime, 0),
	}, nil
}

// Truncate changes the size of a file.
func (sm *SQLiteMount) Truncate(ctx context.Context, p string, size int64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var isDir int
	var fileData []byte
	var currentSize int64

	err := sm.db.QueryRowContext(ctx, `
		SELECT is_dir, data, size FROM vfs_files WHERE path = ?
	`, p).Scan(&isDir, &fileData, &currentSize)

	if err == sql.ErrNoRows {
		return vfs.ErrNotExist
	}
	if err != nil {
		return err
	}

	if isDir == 1 {
		return vfs.ErrIsDirectory
	}

	if size == currentSize {
		return nil
	}

	if size < currentSize {
		// Shrink
		fileData = fileData[:size]
	} else {
		// Expand with zeros
		newData := make([]byte, size)
		copy(newData, fileData)
		fileData = newData
	}

	_, err = sm.db.ExecContext(ctx, `
		UPDATE vfs_files SET data = ?, size = ?, mod_time = ? WHERE path = ?
	`, fileData, size, time.Now().Unix(), p)

	return err
}

// Close closes the database connection.
func (sm *SQLiteMount) Close() error {
	return sm.db.Close()
}

// boolToInt converts a boolean to an integer (0 or 1).
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
