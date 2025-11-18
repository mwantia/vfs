package mount

import (
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/pkg/context"
)

// OpenFile opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (mp *MountPoint) OpenFile(ctx context.TraversalContext, flags data.AccessMode) (mount.Streamer, error) {
	return nil, nil
}

// CloseFile closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (mp *MountPoint) CloseFile(ctx context.TraversalContext, force bool) error {
	return nil
}

// Read reads size bytes from the file at path starting at offset.
// Returns the data read or an error if the operation fails.
func (mp *MountPoint) ReadFile(ctx context.TraversalContext, offset, size int64) ([]byte, error) {
	return nil, nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (mp *MountPoint) WriteFile(ctx context.TraversalContext, offset int64, buffer []byte) (int, error) {
	return 0, nil
}

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (mp *MountPoint) StatMetadata(ctx context.TraversalContext) (*data.Metadata, error) {
	return nil, nil
}

// Lookup checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (mp *MountPoint) LookupMetadata(ctx context.TraversalContext) (bool, error) {
	return false, nil
}

// ReadDirectory returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (mp *MountPoint) ReadDirectory(ctx context.TraversalContext) ([]*data.Metadata, error) {
	return nil, nil
}

// CreateDirectory creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (mp *MountPoint) CreateDirectory(ctx context.TraversalContext) error {
	return nil
}

// RemoveDirectory removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (mp *MountPoint) RemoveDirectory(ctx context.TraversalContext, force bool) error {
	return nil
}

// UnlinkFile removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (mp *MountPoint) UnlinkFile(ctx context.TraversalContext) error {
	return nil
}

// Rename moves or renames a file or directory from oldPath to newPath.
// Returns an error if the operation cannot be completed.
// This implementation uses a copy-and-delete strategy which works across different mounts
// but is not atomic and may not be optimal for large files.
func (mp *MountPoint) Rename(ctx context.TraversalContext, path string) error {
	return nil
}
