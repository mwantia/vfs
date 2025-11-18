package mount

import (
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/pkg/context"
	"github.com/mwantia/vfs/pkg/mount/backend"
)

// VirtualMountSystem is the main VFS manager that handles mount points and delegates
// file operations to the appropriate mount handlers. It provides a Unix-like filesystem
// abstraction with support for nested mounts and thread-safe operations.
type TraversalMountSystem interface {
	// Health returns the basic and fastest result to check the lifecycle and availablility of this backend.
	Health() bool

	// Shutdown unmounts all mounted filesystems and releases all resources.
	// This should be called when shutting down the VFS to ensure proper cleanup.
	// Mounts are unmounted in reverse order (deepest first) to avoid dependency issues.
	Shutdown(ctx context.TraversalContext) error

	// Mount attaches a filesystem handler at the specified path.
	// Options can be used to configure the mount (e.g., read-only).
	Mount(ctx context.TraversalContext, primary backend.ObjectStorageBackend, opts ...mount.MountOption) error

	// Unmount removes the filesystem handler at the specified path.
	// Returns an error if the path is not mounted or has child mounts.
	Unmount(ctx context.TraversalContext, force bool) error

	// OpenFile opens a file with the specified access mode flags and returns a file handle.
	// The returned VirtualFile must be closed by the caller. Use flags to control access.
	OpenFile(ctx context.TraversalContext, flags data.AccessMode) (mount.Streamer, error)

	// CloseFile closes an open file handle at the given path.
	// This may be a no-op for implementations that don't maintain file handles.
	CloseFile(ctx context.TraversalContext, force bool) error

	// Read reads size bytes from the file at path starting at offset.
	// Returns the data read or an error if the operation fails.
	ReadFile(ctx context.TraversalContext, offset, size int64) ([]byte, error)

	// Write writes data to the file at path starting at offset.
	// Returns the number of bytes written or an error if the operation fails.
	WriteFile(ctx context.TraversalContext, offset int64, buffer []byte) (int, error)

	// Stat returns file information for the given path.
	// Returns an error if the path doesn't exist.
	StatMetadata(ctx context.TraversalContext) (*data.Metadata, error)

	// Lookup checks if a file or directory exists at the given path.
	// Returns true if the path exists, false otherwise.
	LookupMetadata(ctx context.TraversalContext) (bool, error)

	// ReadDirectory returns a list of entries in the directory at path.
	// Returns an error if the path is not a directory or doesn't exist.
	ReadDirectory(ctx context.TraversalContext) ([]*data.Metadata, error)

	// CreateDirectory creates a new directory at the specified path.
	// Returns an error if the directory already exists or cannot be created.
	CreateDirectory(ctx context.TraversalContext) error

	// RemoveDirectory removes an empty directory at the specified path.
	// Returns an error if the directory is not empty or doesn't exist.
	RemoveDirectory(ctx context.TraversalContext, force bool) error

	// UnlinkFile removes a file at the specified path.
	// Returns an error if the path is a directory or doesn't exist.
	UnlinkFile(ctx context.TraversalContext) error

	// Rename moves or renames a file or directory from oldPath to newPath.
	// Returns an error if the operation cannot be completed.
	// This implementation uses a copy-and-delete strategy which works across different mounts
	// but is not atomic and may not be optimal for large files.
	Rename(ctx context.TraversalContext, path string) error
}
