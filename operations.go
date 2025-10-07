package vfs

import (
	"context"
	"io"
)

// VirtualOperations defines the interface for advanced VFS operations.
// This interface extends the basic VFS functionality with additional file system operations
// such as random access I/O, attribute management, and extended file operations.
type VirtualOperations interface {
	// OpenRead opens a file for reading and returns a reader.
	// The returned ReadCloser must be closed by the caller.
	OpenRead(ctx context.Context, path string) (io.ReadCloser, error)

	// OpenWrite opens a file for writing and returns a reader/writer.
	// The returned ReadWriteCloser must be closed by the caller.
	OpenWrite(ctx context.Context, path string) (io.ReadWriteCloser, error)

	// Read reads size bytes from the file at path starting at offset.
	// Returns the data read or an error if the operation fails.
	Read(ctx context.Context, path string, offset, size int64) ([]byte, error)

	// Write writes data to the file at path starting at offset.
	// Returns the number of bytes written or an error if the operation fails.
	Write(ctx context.Context, path string, offset int64, data []byte) (int64, error)

	// Close closes an open file handle at the given path.
	// This may be a no-op for implementations that don't maintain file handles.
	Close(ctx context.Context, path string) error

	// MkDir creates a new directory at the specified path.
	// Returns an error if the directory already exists or cannot be created.
	MkDir(ctx context.Context, path string) error

	// RmDir removes an empty directory at the specified path.
	// Returns an error if the directory is not empty or doesn't exist.
	RmDir(ctx context.Context, path string) error

	// Unlink removes a file at the specified path.
	// Returns an error if the path is a directory or doesn't exist.
	Unlink(ctx context.Context, path string) error

	// Rename moves or renames a file or directory from oldPath to newPath.
	// Returns an error if the operation cannot be completed.
	Rename(ctx context.Context, oldPath string, newPath string) error

	// ReadDir returns a list of entries in the directory at path.
	// Returns an error if the path is not a directory or doesn't exist.
	ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error)

	// Stat returns file information for the given path.
	// Returns an error if the path doesn't exist.
	Stat(ctx context.Context, path string) (*VirtualFileInfo, error)

	// Chmod changes the mode (permissions) of the file at path.
	// Returns an error if the operation is not supported or fails.
	Chmod(ctx context.Context, path string, mode VirtualFileMode) error

	// Mount attaches a filesystem handler at the specified path.
	// Options can be used to configure the mount (e.g., read-only).
	Mount(ctx context.Context, path string, mount VirtualMount, opts ...VirtualMountOption) error

	// Unmount removes the filesystem handler at the specified path.
	// Returns an error if the path is not mounted or has child mounts.
	Unmount(ctx context.Context, path string) error

	// Lookup checks if a file or directory exists at the given path.
	// Returns true if the path exists, false otherwise.
	Lookup(ctx context.Context, path string) (bool, error)

	// GetAttr retrieves extended attributes and metadata for the object at path.
	// Returns detailed object information including metadata.
	GetAttr(ctx context.Context, path string) (*VirtualObjectInfo, error)

	// SetAttr updates extended attributes and metadata for the object at path.
	// Returns true if the attributes were updated, false if the path doesn't exist.
	SetAttr(ctx context.Context, path string, info VirtualObjectInfo) (bool, error)
}
