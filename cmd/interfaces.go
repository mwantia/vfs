package cmd

import (
	"context"
	"io"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/backend"
)

// API is a simplified version of FileSystem.
// It strips away all functions not required for command operations.
type API interface {
	// Mount attaches a filesystem handler at the specified path.
	// Options can be used to configure the mount (e.g., read-only).
	Mount(ctx context.Context, path string, primary backend.ObjectStorageBackend, opts ...mount.MountOption) error

	// Unmount removes the filesystem handler at the specified path.
	// Returns an error if the path is not mounted or has child mounts.
	Unmount(ctx context.Context, path string, force bool) error

	// OpenFile opens a file with the specified access mode flags and returns a file handle.
	// The returned File must be closed by the caller. Use flags to control access.
	OpenFile(ctx context.Context, path string, flags data.AccessMode) (mount.Streamer, error)

	// CloseFile closes an open file handle at the given path.
	// This may be a no-op for implementations that don't maintain file handles.
	CloseFile(ctx context.Context, path string, force bool) error

	// Read reads size bytes from the file at path starting at offset.
	// Returns the data read or an error if the operation fails.
	ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error)

	// Write writes data to the file at path starting at offset.
	// Returns the number of bytes written or an error if the operation fails.
	WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error)

	// Stat returns file information for the given path.
	// Returns an error if the path doesn't exist.
	StatMetadata(ctx context.Context, path string) (*data.Metadata, error)

	// Lookup checks if a file or directory exists at the given path.
	// Returns true if the path exists, false otherwise.
	LookupMetadata(ctx context.Context, path string) (bool, error)

	// ReadDirectory returns a list of entries in the directory at path.
	// Returns an error if the path is not a directory or doesn't exist.
	ReadDirectory(ctx context.Context, path string) ([]*data.Metadata, error)

	// CreateDirectory creates a new directory at the specified path.
	// Returns an error if the directory already exists or cannot be created.
	CreateDirectory(ctx context.Context, path string) error

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
	Rename(ctx context.Context, oldPath string, newPath string) error
}

// Command represents an executable command within the virtual filesystem.
type Command interface {
	// Name returns the command identifier
	Name() string

	// Description returns human-readable help text
	Description() string

	// Usage returns a usage string for help (e.g. "ls -al [path]")
	Usage() string

	// Execute runs the command with parsed arguments
	// The writer parameter is where command output should be written
	// Returns exit code (0 = success) and error message
	Execute(ctx context.Context, api API, args *CommandArgs, writer io.Writer) (int, error)

	// GetFlags returns the flag set for this command (this is optional)
	GetFlags() *CommandFlagSet
}
