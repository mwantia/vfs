package tui

import (
	"context"
	"io"
	"path/filepath"
	"strings"

	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/data"
)

// VFSAdapter wraps VirtualFileSystem operations for the TUI
type VFSAdapter struct {
	vfs *vfs.VirtualFileSystem
	ctx context.Context
}

// NewVFSAdapter creates a new adapter for VFS operations
func NewVFSAdapter(ctx context.Context, fs *vfs.VirtualFileSystem) *VFSAdapter {
	return &VFSAdapter{
		vfs: fs,
		ctx: ctx,
	}
}

// ListDirectory returns entries in the specified directory
func (a *VFSAdapter) ListDirectory(path string) ([]*Entry, error) {
	metas, err := a.vfs.ReadDirectory(a.ctx, path)
	if err != nil {
		// Special case: if root directory read fails, it might not exist as an entry
		// Try to infer children by attempting to stat known common directories
		if path == "/" && err == data.ErrNotExist {
			return a.inferRootChildren()
		}
		return nil, err
	}

	// Normalize path for comparison - backend keys are relative to mount point
	// So /etc/config.conf is stored as "etc/config.conf" in the backend
	normalizedPath := strings.TrimPrefix(path, "/")
	if normalizedPath != "" && !strings.HasSuffix(normalizedPath, "/") {
		normalizedPath = normalizedPath + "/"
	}

	// Track seen entries to avoid duplicates
	seen := make(map[string]bool)
	entries := make([]*Entry, 0, len(metas))

	for _, meta := range metas {
		// Keys in metadata are relative to mount point (without leading /)
		// For path "/etc", normalizedPath is "etc/"
		// meta.Key might be "etc/config.conf"

		// Strip the directory prefix from the key
		relativeKey := meta.Key
		if normalizedPath != "" {
			if !strings.HasPrefix(meta.Key, normalizedPath) {
				// This entry is not in the current directory
				continue
			}
			relativeKey = strings.TrimPrefix(meta.Key, normalizedPath)
		}

		// Skip if empty (current directory itself)
		if relativeKey == "" {
			continue
		}

		// Extract only the direct child name (first path component after current dir)
		parts := strings.Split(relativeKey, "/")
		directChild := parts[0]

		// Skip if we've already seen this entry
		if seen[directChild] {
			continue
		}
		seen[directChild] = true

		// Determine if this is a directory by checking if there are more parts
		// or if the mode indicates it's a directory
		isDir := len(parts) > 1 || meta.Mode.IsDir()

		// Build the full absolute path for VFS operations
		fullPath := filepath.Join(path, directChild)

		entry := &Entry{
			Name:     directChild,
			Path:     fullPath,
			Size:     meta.Size,
			Mode:     meta.Mode,
			ModTime:  meta.ModifyTime,
			IsDir:    isDir,
			MimeType: meta.ContentType,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Stat returns information about a file or directory
func (a *VFSAdapter) Stat(path string) (*Entry, error) {
	meta, err := a.vfs.StatMetadata(a.ctx, path)
	if err != nil {
		return nil, err
	}

	entry := &Entry{
		Name:     filepath.Base(meta.Key),
		Path:     path,
		Size:     meta.Size,
		Mode:     meta.Mode,
		ModTime:  meta.ModifyTime,
		IsDir:    meta.Mode.IsDir(),
		MimeType: meta.ContentType,
	}

	return entry, nil
}

// ReadFileContent reads the content of a file for preview
func (a *VFSAdapter) ReadFileContent(path string, maxBytes int64) (string, error) {
	// Get file info first to check size
	meta, err := a.vfs.StatMetadata(a.ctx, path)
	if err != nil {
		return "", err
	}

	if meta.Mode.IsDir() {
		return "", data.ErrIsDirectory
	}

	// Limit read size
	readSize := meta.Size
	if readSize > maxBytes {
		readSize = maxBytes
	}

	if readSize == 0 {
		return "", nil
	}

	// Read file content
	content, err := a.vfs.ReadFile(a.ctx, path, 0, readSize)
	if err != nil {
		return "", err
	}

	// Convert to string, replacing non-printable characters
	return sanitizeContent(string(content)), nil
}

// CreateDirectory creates a new directory
func (a *VFSAdapter) CreateDirectory(path string) error {
	return a.vfs.CreateDirectory(a.ctx, path)
}

// CreateFile creates a new empty file
func (a *VFSAdapter) CreateFile(path string) error {
	file, err := a.vfs.OpenFile(a.ctx, path, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeExcl)
	if err != nil {
		return err
	}
	defer file.Close()
	return nil
}

// Delete removes a file or directory
func (a *VFSAdapter) Delete(path string, isDir bool) error {
	if isDir {
		return a.vfs.RemoveDirectory(a.ctx, path, false)
	}
	return a.vfs.UnlinkFile(a.ctx, path)
}

// DeleteRecursive removes a directory and all its contents
func (a *VFSAdapter) DeleteRecursive(path string) error {
	return a.vfs.RemoveDirectory(a.ctx, path, true)
}

// Exists checks if a path exists
func (a *VFSAdapter) Exists(path string) bool {
	exists, _ := a.vfs.Lookup(a.ctx, path)
	return exists
}

// sanitizeContent replaces non-printable characters for display
func sanitizeContent(content string) string {
	var builder strings.Builder
	for _, r := range content {
		if r == '\n' || r == '\r' || r == '\t' || (r >= 32 && r < 127) || r >= 160 {
			builder.WriteRune(r)
		} else {
			builder.WriteRune('�')
		}
	}
	return builder.String()
}

// WriteFile writes content to a file
func (a *VFSAdapter) WriteFile(path string, content []byte) error {
	file, err := a.vfs.OpenFile(a.ctx, path, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeTrunc)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(content)
	return err
}

// CopyFile copies a file from src to dst
func (a *VFSAdapter) CopyFile(src, dst string) error {
	// Read source file
	srcMeta, err := a.vfs.StatMetadata(a.ctx, src)
	if err != nil {
		return err
	}

	if srcMeta.Mode.IsDir() {
		return data.ErrIsDirectory
	}

	content, err := a.vfs.ReadFile(a.ctx, src, 0, srcMeta.Size)
	if err != nil {
		return err
	}

	// Write to destination
	dstFile, err := a.vfs.OpenFile(a.ctx, dst, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeTrunc)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = dstFile.Write(content)
	return err
}

// StreamFile opens a file for streaming read operations
func (a *VFSAdapter) StreamFile(path string) (io.ReadCloser, error) {
	file, err := a.vfs.OpenFile(a.ctx, path, data.AccessModeRead)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// inferRootChildren attempts to discover root-level directories
// This is a workaround for when the root directory entry doesn't exist
func (a *VFSAdapter) inferRootChildren() ([]*Entry, error) {
	// Try common directory names
	commonDirs := []string{"home", "etc", "var", "tmp", "usr", "opt", "data"}

	entries := make([]*Entry, 0)

	for _, name := range commonDirs {
		path := "/" + name
		meta, err := a.vfs.StatMetadata(a.ctx, path)
		if err != nil {
			// Skip if doesn't exist
			continue
		}

		entry := &Entry{
			Name:     name,
			Path:     path,
			Size:     meta.Size,
			Mode:     meta.Mode,
			ModTime:  meta.ModifyTime,
			IsDir:    meta.Mode.IsDir(),
			MimeType: meta.ContentType,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
