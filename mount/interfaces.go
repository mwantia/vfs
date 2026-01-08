package mount

import (
	"context"
	"io"

	traversal "github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/builder"
)

// VirtualMountSystem is the main VFS manager that handles mount points and delegates
// file operations to the appropriate mount handlers. It provides a Unix-like filesystem
// abstraction with support for nested mounts and thread-safe operations.
type MountPoint interface {
	// Health returns the basic and fastest result to check the lifecycle and availablility of this backend.
	Health() bool

	// IsBusy
	IsBusy() bool

	// Shutdown unmounts all mounted filesystems and releases all resources.
	// This should be called when shutting down the VFS to ensure proper cleanup.
	// Mounts are unmounted in reverse order (deepest first) to avoid dependency issues.
	Shutdown(ctx context.Context) error

	// Mount attaches a filesystem handler at the specified path.
	// Options can be used to configure the mount (e.g., read-only).
	Mount(ctx context.Context, path string, steps ...builder.MountStep) error

	// Unmount removes the filesystem handler at the specified path.
	// Returns an error if the path is not mounted or has child mounts.
	Unmount(ctx context.Context, path string, force bool) error

	// Restore
	Restore(ctx context.Context) error

	// Read reads size bytes from the file at path starting at offset.
	// Returns the data read or an error if the operation fails.
	ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error)

	// Write writes data to the file at path starting at offset.
	// Returns the number of bytes written or an error if the operation fails.
	WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error)

	// StatMetadata returns file information for the given path.
	// Returns an error if the path doesn't exist.
	StatMetadata(ctx context.Context, path string) (*data.Metadata, error)

	// Lookup checks if a file or directory exists at the given path.
	// Returns true if the path exists, false otherwise.
	LookupMetadata(ctx context.Context, path string, quick bool) (bool, error)

	// ReadDirectory returns a list of entries in the directory at path.
	// Returns an error if the path is not a directory or doesn't exist.
	ReadDirectory(ctx context.Context, path string) ([]*data.Metadata, error)

	// CreateDirectory creates a new directory at the specified path.
	// Returns an error if the directory already exists or cannot be created.
	CreateDirectory(ctx context.Context, path string) (*data.Metadata, error)

	// RemoveDirectory removes an empty directory at the specified path.
	// Returns an error if the directory is not empty or doesn't exist.
	RemoveDirectory(ctx context.Context, path string, force bool) error

	// UnlinkFile removes a file at the specified path.
	// Returns an error if the path is a directory or doesn't exist.
	UnlinkFile(ctx context.Context, path string) error

	// Rename moves or renames a file or directory from oldPath to newPath.
	// Returns an error if the operation cannot be completed.
	// This implementation uses a copy-and-delete strategy which works across different mounts
	// but is not atomic and may not be optimal for large files.
	Rename(ctx context.Context, sourcePath, targetPath string) error

	// OpenFile opens a file with the specified access mode flags and returns a file handle.
	// The returned VirtualFile must be closed by the caller. Use flags to control access.
	OpenFile(traversal traversal.TraversalContext, flags data.AccessMode) (MountStreamer, error)

	// CloseFile closes an open file handle at the given path.
	// This may be a no-op for implementations that don't maintain file handles.
	CloseFile(traversal traversal.TraversalContext, force bool) error
}

// Streamer combines all operation interfaces for data-streaming.
// It provides read, write, seek, and close capabilities.
// The available operations depend on the access mode flags used when opening.
type MountStreamer interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer

	// IsBusy tries to return the current state of the stream.
	// It should be used to determine, if it's safe to close a stream.
	IsBusy() bool

	// CanRead returns true if the virtual file can be read, otherwise false.
	CanRead() bool

	// CanWrite returns true if the virtual file can be written, otherwise false.
	CanWrite() bool
}
