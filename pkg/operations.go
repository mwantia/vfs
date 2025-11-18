package pkg

import (
	"context"
	"fmt"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
	tctx "github.com/mwantia/vfs/pkg/context"
)

// OpenFile opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (vfs *virtualFileSystemImpl) OpenFile(ctx context.Context, path string, flags data.AccessMode) (mount.Streamer, error) {
	root, err := vfs.checkRootMount()
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", path, err)
	}

	return root.OpenFile(tctx.WithAbsolute(ctx, path), flags)
}

// CloseFile closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (vfs *virtualFileSystemImpl) CloseFile(ctx context.Context, path string, force bool) error {
	root, err := vfs.checkRootMount()
	if err != nil {
		return fmt.Errorf("failed to close file '%s': %w", path, err)
	}

	return root.CloseFile(tctx.WithAbsolute(ctx, path), force)
}

// Read reads size bytes from the file at path starting at offset.
// Returns the data read or an error if the operation fails.
func (vfs *virtualFileSystemImpl) ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error) {
	root, err := vfs.checkRootMount()
	if err != nil {
		return nil, fmt.Errorf("failed to read file '%s': %w", path, err)
	}

	return root.ReadFile(tctx.WithAbsolute(ctx, path), offset, size)
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (vfs *virtualFileSystemImpl) WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error) {
	root, err := vfs.checkRootMount()
	if err != nil {
		return 0, fmt.Errorf("failed to write file '%s': %w", path, err)
	}

	return root.WriteFile(tctx.WithAbsolute(ctx, path), offset, buffer)
}

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (vfs *virtualFileSystemImpl) StatMetadata(ctx context.Context, path string) (*data.Metadata, error) {
	root, err := vfs.checkRootMount()
	if err != nil {
		return nil, fmt.Errorf("failed to stat metadata '%s': %w", path, err)
	}

	return root.StatMetadata(tctx.WithAbsolute(ctx, path))
}

// Lookup checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (vfs *virtualFileSystemImpl) LookupMetadata(ctx context.Context, path string) (bool, error) {
	root, err := vfs.checkRootMount()
	if err != nil {
		return false, fmt.Errorf("failed to lookup metadata '%s': %w", path, err)
	}

	return root.LookupMetadata(tctx.WithAbsolute(ctx, path))
}

// ReadDirectory returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (vfs *virtualFileSystemImpl) ReadDirectory(ctx context.Context, path string) ([]*data.Metadata, error) {
	root, err := vfs.checkRootMount()
	if err != nil {
		return nil, fmt.Errorf("failed to read directory '%s': %w", path, err)
	}

	return root.ReadDirectory(tctx.WithAbsolute(ctx, path))
}

// CreateDirectory creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (vfs *virtualFileSystemImpl) CreateDirectory(ctx context.Context, path string) error {
	root, err := vfs.checkRootMount()
	if err != nil {
		return fmt.Errorf("failed to create directory '%s': %w", path, err)
	}

	return root.CreateDirectory(tctx.WithAbsolute(ctx, path))
}

// RemoveDirectory removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (vfs *virtualFileSystemImpl) RemoveDirectory(ctx context.Context, path string, force bool) error {
	root, err := vfs.checkRootMount()
	if err != nil {
		return fmt.Errorf("failed to remove directory '%s': %w", path, err)
	}

	return root.RemoveDirectory(tctx.WithAbsolute(ctx, path), force)
}

// UnlinkFile removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (vfs *virtualFileSystemImpl) UnlinkFile(ctx context.Context, path string) error {
	root, err := vfs.checkRootMount()
	if err != nil {
		return fmt.Errorf("failed to unlink file '%s': %w", path, err)
	}

	return root.UnlinkFile(tctx.WithAbsolute(ctx, path))
}

// Rename moves or renames a file or directory from oldPath to newPath.
// Returns an error if the operation cannot be completed.
// This implementation uses a copy-and-delete strategy which works across different mounts
// but is not atomic and may not be optimal for large files.
func (vfs *virtualFileSystemImpl) Rename(ctx context.Context, oldPath string, newPath string) error {
	root, err := vfs.checkRootMount()
	if err != nil {
		return fmt.Errorf("failed to rename '%s' to '%s': %w", oldPath, newPath, err)
	}

	return root.Rename(tctx.WithAbsolute(ctx, oldPath), newPath)
}
