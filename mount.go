package vfs

import (
	"context"
	"io"
	"time"
)

// VirtualMount represents a mounted filesystem.
// Implementations provide access to a specific storage backend.
// All paths passed to Mount methods are relative to the mount point.
type VirtualMount interface {
	// Geturns a list of supported capabilities for this mount
	GetCapabilities() VirtualMountCapabilities

	// Stat returns information about a virtual object.
	// Returns ErrNotExist if the path doesn't exist.
	Stat(ctx context.Context, path string) (*VirtualObjectInfo, error)

	// List returns all virtual objects under the given path.
	// For files, returns single entry. For directory, returns children.
	List(ctx context.Context, path string) ([]*VirtualObjectInfo, error)

	// Get retrieves a virtual object and its metadata information.
	Get(ctx context.Context, path string) (*VirtualObject, error)

	// Create will create a new virtual object.
	// Returns ErrExist if the path already exist.
	Create(ctx context.Context, path string, obj *VirtualObject) error

	// Update will update an existing virtual object.
	// Returns false (no error), if the path doesn't exist.
	Update(ctx context.Context, path string, obj *VirtualObject) (bool, error)

	// Delete removes an virtual object.
	// If force is true and object is directory, removes all children.
	Delete(ctx context.Context, path string, force bool) (bool, error)

	// Upsert either creates or updates a virtual object.
	// It uses Stat to determine if the virtual object already exists or not.
	Upsert(ctx context.Context, path string, source any) error
}

// Mount represents a mounted filesystem handler.
// Implementations provide access to a specific storage backend.
// All paths passed to Mount methods are relative to the mount point.
type OldVirtualMount interface {
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
