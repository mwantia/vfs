package vfs

import (
	"context"
	"io"
	"time"
)

// Mount represents a mounted filesystem handler.
// Implementations provide access to a specific storage backend.
// All paths passed to Mount methods are relative to the mount point.
type VirtualMount interface {
	// Stat returns information about a file or directory.
	// Returns ErrNotExist if the path does not exist.
	Stat(ctx context.Context, path string) (*VirtualFileInfo, error)

	// ReadDir lists directory contents.
	// Returns a slice of FileInfo for each entry in the directory.
	// Returns ErrNotExist if the directory does not exist.
	// Returns ErrNotDirectory if the path is not a directory.
	ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error)

	// Open opens a file for reading.
	// Returns ErrNotExist if the file does not exist.
	// Returns ErrIsDirectory if the path is a directory.
	Open(ctx context.Context, path string) (io.ReadCloser, error)

	// Create creates a new file for writing.
	// If the file already exists, it is truncated.
	// Returns ErrReadOnly if the mount is read-only.
	Create(ctx context.Context, path string) (io.WriteCloser, error)

	// Remove deletes a file.
	// Returns ErrNotExist if the file does not exist.
	// Returns ErrIsDirectory if the path is a directory.
	// Returns ErrReadOnly if the mount is read-only.
	Remove(ctx context.Context, path string) error

	// Mkdir creates a directory.
	// Returns ErrExist if the directory already exists.
	// Returns ErrReadOnly if the mount is read-only.
	Mkdir(ctx context.Context, path string) error

	// RemoveAll removes a directory and all its contents.
	// Returns ErrNotExist if the path does not exist.
	// Returns ErrReadOnly if the mount is read-only.
	RemoveAll(ctx context.Context, path string) error
}

// MountInfo provides metadata about a mounted filesystem.
type VirtualMountInfo struct {
	Path      string    // Mount point path (e.g., "/data")
	ReadOnly  bool      // Whether the mount is read-only
	MountedAt time.Time // When the mount was created
}

// MountOption configures mount behavior.
type VirtualMountOption func(*VirtualMountInfo)

// WithReadOnly sets whether the mount is read-only.
func WithReadOnly(ro bool) VirtualMountOption {
	return func(info *VirtualMountInfo) {
		info.ReadOnly = ro
	}
}

// WithType sets the mount type for metadata purposes.
func WithType(typ string) VirtualMountOption {
	return func(info *VirtualMountInfo) {

	}
}
