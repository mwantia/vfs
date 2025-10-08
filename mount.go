package vfs

import (
	"context"
	"time"
)

// VirtualMount represents a mounted filesystem.
// Implementations provide access to a specific storage backend.
// All paths passed to Mount methods are relative to the mount point.
type VirtualMount interface {
	// GetCapabilities returns a list of supported capabilities for this mount.
	GetCapabilities() VirtualMountCapabilities

	// Stat returns information about a virtual object.
	// Returns ErrNotExist if the path doesn't exist.
	Stat(ctx context.Context, path string) (*VirtualObjectInfo, error)

	// List returns all virtual objects under the given path.
	// For directories, returns all direct children.
	// For files, returns single entry with the file's info.
	// Returns ErrNotExist if the path doesn't exist.
	List(ctx context.Context, path string) ([]*VirtualObjectInfo, error)

	// Read reads up to len(data) bytes from the object at path starting at offset.
	// Returns the number of bytes read and any error encountered.
	// Returns ErrNotExist if the path doesn't exist.
	// Returns ErrIsDirectory if the path is a directory.
	// If offset is beyond the file size, returns 0, io.EOF.
	Read(ctx context.Context, path string, offset int64, data []byte) (int, error)

	// Write writes data to the object at path starting at offset.
	// If offset is beyond current file size, the gap is filled with zeros.
	// Returns the number of bytes written and any error encountered.
	// Returns ErrNotExist if the path doesn't exist (use Create first).
	// Returns ErrIsDirectory if the path is a directory.
	Write(ctx context.Context, path string, offset int64, data []byte) (int, error)

	// Create creates a new file or directory at the given path.
	// For files, isDir should be false. For directories, isDir should be true.
	// Returns ErrExist if the path already exists.
	// Parent directories are NOT created automatically - they must exist.
	Create(ctx context.Context, path string, isDir bool) error

	// Delete removes the object at the given path.
	// If force is true and the object is a directory, removes all children recursively.
	// If force is false and the directory is not empty, returns an error.
	// Returns ErrNotExist if the path doesn't exist.
	Delete(ctx context.Context, path string, force bool) error

	// Truncate changes the size of the file at path.
	// If the file is larger than size, the extra data is discarded.
	// If the file is smaller than size, it is extended with zero bytes.
	// Returns ErrNotExist if the path doesn't exist.
	// Returns ErrIsDirectory if the path is a directory.
	Truncate(ctx context.Context, path string, size int64) error
}

// VirtualMountOptions provides metadata about a mounted filesystem.
type VirtualMountOptions struct {
	// Mount point path (e.g., "/data").
	Path string

	// When the mount was created.
	MountTime time.Time

	// Whether the mount is read-only.
	ReadOnly bool

	// Whether the mount allows for nested mountpoints.
	DenyNesting bool
}

// VirtualMountOption configures mount behavior.
type VirtualMountOption func(*VirtualMountOptions)

// WithAbsolutePath defines or overwrites the absolute path used for this mount
func WithAbsolutePath(path string) VirtualMountOption {
	return func(vmo *VirtualMountOptions) {
		absPath, err := ToAbsolutePath(path)
		if err != nil {
			panic(err)
		}

		vmo.Path = absPath
	}
}

// WithMountTime defines or overwrites the mounttime for this mount
func WithMountTime(mountTime time.Time) VirtualMountOption {
	return func(vmo *VirtualMountOptions) {
		vmo.MountTime = mountTime
	}
}

// WithDenyNesting specifies, if nested mountpoints are allowed within this mount
func WithDenyNesting(denyNesting bool) VirtualMountOption {
	return func(vmo *VirtualMountOptions) {
		vmo.DenyNesting = denyNesting
	}
}

// Using AsReadOnly sets the mount in a readonly state
func AsReadOnly(ro bool) VirtualMountOption {
	return func(vmo *VirtualMountOptions) {
		vmo.ReadOnly = ro
	}
}
