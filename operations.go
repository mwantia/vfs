package vfs

import (
	"context"
	"strings"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/errors"
	"github.com/mwantia/vfs/mount"
)

// OpenFile opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (vfs *virtualFileSystemImpl) OpenFile(ctx context.Context, path string, flags data.AccessMode, opts ...mount.MountStreamerOption) (mount.Streamer, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return nil, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return nil, errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	return root.OpenFile(ctx, relativePath, flags, opts...)
}

// CloseFile closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (vfs *virtualFileSystemImpl) CloseFile(ctx context.Context, path string, force bool) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	return root.CloseFile(ctx, relativePath, force)
}

// Read reads size bytes from the file at path starting at offset.
// Returns the data read or an error if the operation fails.
func (vfs *virtualFileSystemImpl) ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return nil, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return nil, errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	return root.ReadFile(ctx, relativePath, offset, size)
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (vfs *virtualFileSystemImpl) WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return 0, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return 0, errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	return root.WriteFile(ctx, relativePath, offset, buffer)
}

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (vfs *virtualFileSystemImpl) StatMetadata(ctx context.Context, path string) (data.Metadata, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return data.Metadata{}, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return data.Metadata{}, errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	return root.StatMetadata(ctx, relativePath)
}

// Lookup checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (vfs *virtualFileSystemImpl) LookupMetadata(ctx context.Context, path string) (bool, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return false, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return false, errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	return root.LookupMetadata(ctx, relativePath, false)
}

// ReadDirectory returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (vfs *virtualFileSystemImpl) ReadDirectory(ctx context.Context, path string) ([]data.Metadata, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return nil, err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return nil, errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	metas, err := root.ReadDirectory(ctx, relativePath)

	return metas, err
}

// CreateDirectory creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (vfs *virtualFileSystemImpl) CreateDirectory(ctx context.Context, path string) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	_, err = root.CreateDirectory(ctx, relativePath)
	return err
}

// RemoveDirectory removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (vfs *virtualFileSystemImpl) RemoveDirectory(ctx context.Context, path string, force bool) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	return root.RemoveDirectory(ctx, relativePath, force)
}

// UnlinkFile removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (vfs *virtualFileSystemImpl) UnlinkFile(ctx context.Context, path string) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativePath := strings.TrimPrefix(path, "/")
	return root.UnlinkFile(ctx, relativePath)
}

// Rename moves or renames a file or directory from oldPath to newPath.
// Returns an error if the operation cannot be completed.
// This implementation uses a copy-and-delete strategy which works across different mounts
// but is not atomic and may not be optimal for large files.
func (vfs *virtualFileSystemImpl) Rename(ctx context.Context, oldPath string, newPath string) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}

	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootOperation
	}
	/* TODO :: Does it suffice to traverse the request to root and be done,
	 *         or are there any additional operations we need to do? */
	relativeOldPath := strings.TrimPrefix(oldPath, "/")
	relativeNewPath := strings.TrimPrefix(newPath, "/")
	return root.Rename(ctx, relativeOldPath, relativeNewPath)
}
