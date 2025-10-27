package vfs

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/mwantia/vfs/backend"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
)

// VirtualFileSystem is the main VFS manager that handles mount points and delegates
// file operations to the appropriate mount handlers. It provides a Unix-like filesystem
// abstraction with support for nested mounts and thread-safe operations.
type VirtualFileSystem struct {
	mu   sync.RWMutex
	mnts map[string]*mount.Mount
}

// NewVfs creates a new VirtualFileSystem instance with no initial mounts.
func NewVfs() *VirtualFileSystem {
	return &VirtualFileSystem{
		mnts: make(map[string]*mount.Mount),
	}
}

// Open opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (vfs *VirtualFileSystem) OpenFile(ctx context.Context, path string, flags data.VirtualAccessMode) (mount.Streamer, error) {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return nil, err
	}

	relative := ToRelativePath(absolute, mnt.Path)
	// Check if any previous streamer exists
	streamer, exists := mnt.GetStreamer(relative)
	if exists {
		return streamer, nil
	}
	// Fail if we expect to create or write a stream on a readonly mount
	if mnt.Options.ReadOnly {
		if flags&data.AccessModeWrite != 0 || flags&data.AccessModeCreate != 0 || flags&data.AccessModeExcl != 0 {
			return nil, data.ErrReadOnly
		}
	}
	// Determine initial offset
	offset := int64(0)
	// We need to determine if the file exists
	if mnt.Metadata != nil {
		// Try to read info from metadata
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				return nil, err
			}
			// Fallback to storage to read object stats
			stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
			if err != nil {
				// Fail if any error except NotExists
				if err != data.ErrNotExist {
					return nil, err
				}
				// Return when the file shouldn't be created
				if !flags.HasCreate() {
					return nil, err
				}
				stat, err = mnt.ObjectStorage.CreateObject(ctx, relative, 0x777)
				if err != nil {
					return nil, err
				}
			}
			// Convert object stats to metadata
			meta = stat.ToMetadata()
			if !mnt.IsDualMount {
				// Write stat back into metadata
				if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
					return nil, err
				}
			}
		}
		// File exists - check EXCL flag
		if flags.HasExcl() && flags.HasCreate() {
			return nil, data.ErrExist
		}
		// Unable to open directories as streams
		if meta.Mode.IsDir() {
			return nil, data.ErrIsDirectory
		}
		// Read filesize from metadata if append
		if flags.HasAppend() {
			offset = meta.Size
		}
	} else {
		// Fallback to storage to read object stats
		stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				return nil, err
			}
			// Return when we file shouldn't be created
			if !flags.HasCreate() {
				return nil, err
			}
			stat, err = mnt.ObjectStorage.CreateObject(ctx, relative, 0x777)
			if err != nil {
				return nil, err
			}
		}
		// File exists - check EXCL flag
		if flags.HasExcl() && flags.HasCreate() {
			return nil, data.ErrExist
		}
		// Unable to open directories as streams
		if stat.Mode.IsDir() {
			return nil, data.ErrIsDirectory
		}
		// Read filesize from metadata if append
		if flags.HasAppend() {
			offset = stat.Size
		}
	}
	// Only truncate if TRUNC flag is set and we have write access
	if flags.HasTrunc() && (flags.IsWriteOnly() || flags.IsReadWrite()) {
		// TODO :: Find a way to reliably truncate data
		fmt.Printf("")
	}

	return mnt.OpenStreamer(ctx, relative, offset, flags), nil
}

// Close closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (vfs *VirtualFileSystem) CloseFile(ctx context.Context, path string, force bool) error {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return err
	}

	relative := ToRelativePath(absolute, mnt.Path)
	return mnt.CloseStreamer(ctx, relative, force)
}

// Read reads size bytes from the file at path starting at offset.
// Returns the data read or an error if the operation fails.
func (vfs *VirtualFileSystem) ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error) {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}
	// Fail immediately, if we receive an invalid size
	if size <= 0 {
		return nil, fmt.Errorf("vfs: size out of range")
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return nil, err
	}

	relative := ToRelativePath(absolute, mnt.Path)
	// If metadata exists, validate if size and offset matches
	if mnt.Metadata != nil {
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				return nil, err
			}
			// Fallback to storage to read object stats
			stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
			if err != nil {
				return nil, err
			}
			// Convert object stats to metadata
			meta = stat.ToMetadata()
			if !mnt.IsDualMount {
				// Write stat back into metadata
				if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
					return nil, err
				}
			}
		}
		// Ignore files, which cannot be read
		if meta.Mode.IsDir() {
			return nil, data.ErrIsDirectory
		}
		// Validate filesize
		if (offset + size) > meta.Size {
			return nil, fmt.Errorf("vfs: size out of range")
		}
	} else {
		// Fallback to storage to read object stats
		stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			return nil, err
		}
		// Ignore files, which cannot be read
		if stat.Mode.IsDir() {
			return nil, data.ErrIsDirectory
		}
		// Validate filesize
		if (offset + size) > stat.Size {
			return nil, fmt.Errorf("vfs: size out of range")
		}
	}

	buffer := make([]byte, size)
	n, err := mnt.ObjectStorage.ReadObject(ctx, relative, offset, buffer)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return buffer[:n], nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (vfs *VirtualFileSystem) WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error) {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return 0, err
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return 0, err
	}
	// Fail if mount is readonly
	if mnt.Options.ReadOnly {
		return 0, data.ErrReadOnly
	}

	relative := ToRelativePath(absolute, mnt.Path)
	// Read metadata info if available
	if mnt.Metadata != nil {
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				return 0, err
			}
			// Fallback to storage to read object stats
			stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
			if err != nil {
				return 0, err
			}
			// Convert object stats to metadata
			meta = stat.ToMetadata()
			if !mnt.IsDualMount {
				// Write stat back into metadata
				if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
					return 0, err
				}
			}
		}
		// Ignore files, which cannot be read
		if meta.Mode.IsDir() {
			return 0, data.ErrIsDirectory
		}
	} else {
		// Fallback to storage to read object stats
		stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			return 0, err
		}
		// Ignore files, which cannot be read
		if stat.Mode.IsDir() {
			return 0, data.ErrIsDirectory
		}
	}

	n, err := mnt.ObjectStorage.WriteObject(ctx, relative, offset, buffer)
	if err != nil {
		return 0, err
	}

	// Sync metadata information after successfull write
	if mnt.Metadata != nil && !mnt.IsDualMount {
		update := &data.VirtualFileMetadataUpdate{
			Mask: data.VirtualFileMetadataUpdateSize,
			Metadata: &data.VirtualFileMetadata{
				Size: offset + int64(n),
			},
		}
		if err := mnt.Metadata.UpdateMeta(ctx, relative, update); err != nil {
			return n, err
		}
	}

	return n, err
}

// CreateDirectory creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (vfs *VirtualFileSystem) CreateDirectory(ctx context.Context, path string) error {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return err
	}
	// Fail if mount is readonly
	if mnt.Options.ReadOnly {
		return data.ErrReadOnly
	}

	relative := ToRelativePath(absolute, mnt.Path)
	// Check if path exists in metadata
	if mnt.Metadata != nil {
		if exists, _ := mnt.Metadata.ExistsMeta(ctx, relative); exists {
			return data.ErrExist
		}
	}

	// Create folder in object storage
	stat, err := mnt.ObjectStorage.CreateObject(ctx, relative, data.ModeDir|0x777)
	if err != nil {
		// Fail if any error except Exists
		if err != data.ErrExist {
			return err
		}
	}
	// Write back into metadata (only if separate metadata backend)
	if mnt.Metadata != nil && !mnt.IsDualMount {
		// In case stat hasn't been provided from 'CreateObject'
		if stat == nil {
			stat, err = mnt.ObjectStorage.HeadObject(ctx, relative)
			if err != nil {
				return err
			}
		}

		meta := stat.ToMetadata()
		if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
			return err
		}
	}

	return nil
}

// RemoveDirectory removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (vfs *VirtualFileSystem) RemoveDirectory(ctx context.Context, path string, force bool) error {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return err
	}
	// Fail if mount is readonly
	if mnt.Options.ReadOnly {
		return data.ErrReadOnly
	}

	relative := ToRelativePath(absolute, mnt.Path)

	var stat *data.VirtualFileStat
	// Check if path exists in metadata
	if mnt.Metadata != nil {
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			return err
		}

		stat = meta.ToStat()
	} else {
		stat, err = mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			return err
		}
	}
	// RemoveDirectory is only supposed to delete directories
	if !stat.Mode.IsDir() {
		return data.ErrNotDirectory
	}

	if !force {
		if mnt.Metadata != nil {
			query := &backend.MetadataQuery{
				Prefix:    relative + "/",
				Delimiter: "/",
				Limit:     1, // Only need to know if ANY children exist
			}

			result, err := mnt.Metadata.QueryMeta(ctx, query)
			if err != nil {
				return err
			}

			if result != nil && result.TotalCount > 0 {
				return data.ErrDirectoryNotEmpty
			}
		} else {
			// TODO :: We need to check object storage if the directory is empty
		}
	}

	// Delete directory from object storage
	// Force to specifically delete directories
	if err := mnt.ObjectStorage.DeleteObject(ctx, relative, true); err != nil {
		return err
	}
	// Sync deletion to metadata if available
	if mnt.Metadata != nil && !mnt.IsDualMount {
		// TODO :: For directories we need to delete all child metadata
		if err := mnt.Metadata.DeleteMeta(ctx, relative); err != nil {
			return err
		}
	}

	return nil
}

// UnlinkFile removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (vfs *VirtualFileSystem) UnlinkFile(ctx context.Context, path string) error {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return err
	}
	// Fail if mount is readonly
	if mnt.Options.ReadOnly {
		return data.ErrReadOnly
	}

	relative := ToRelativePath(absolute, mnt.Path)

	var stat *data.VirtualFileStat
	// Check if path exists in metadata
	if mnt.Metadata != nil {
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			return err
		}

		stat = meta.ToStat()
	} else {
		stat, err = mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			return err
		}
	}
	// Unlink is unable to delete directories
	if stat.Mode.IsDir() {
		return data.ErrIsDirectory
	}
	// Delete file from object storage
	// Force to specifically deletes directories
	if err := mnt.ObjectStorage.DeleteObject(ctx, relative, false); err != nil {
		return err
	}
	// Sync deletion to metadata if available
	if mnt.Metadata != nil && !mnt.IsDualMount {
		if err := mnt.Metadata.DeleteMeta(ctx, relative); err != nil {
			return err
		}
	}

	return nil
}

// Rename moves or renames a file or directory from oldPath to newPath.
// Returns an error if the operation cannot be completed.
func (vfs *VirtualFileSystem) Rename(ctx context.Context, oldPath string, newPath string) error {
	return fmt.Errorf("vfs: not implemented")
}

// ReadDirectory returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (vfs *VirtualFileSystem) ReadDirectory(ctx context.Context, path string) ([]*data.VirtualFileMetadata, error) {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return nil, err
	}

	relative := ToRelativePath(absolute, mnt.Path)

	if mnt.Metadata != nil {
		query := &backend.MetadataQuery{
			Prefix:    relative + "/",
			SortBy:    backend.SortByKey,
			SortOrder: backend.SortAsc,
		}

		result, err := mnt.Metadata.QueryMeta(ctx, query)
		if err != nil {
			return nil, err
		}

		if result != nil && result.TotalCount > 0 {
			return result.Candidates, nil
		}
	}

	// Use storage backend to list objects
	stats, err := mnt.ObjectStorage.ListObjects(ctx, relative)
	if err != nil {
		return nil, err
	}

	// Convert stats to metadata
	metas := make([]*data.VirtualFileMetadata, 0, len(stats))
	for _, stat := range stats {
		meta := stat.ToMetadata()
		// Sync to metadata if available AND it's a separate backend instance
		if mnt.Metadata != nil && !mnt.IsDualMount {
			if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
				continue // Ignore errors for existing metadata
			}
		}

		metas = append(metas, meta)
	}

	return metas, nil
}

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (vfs *VirtualFileSystem) StatMetadata(ctx context.Context, path string) (*data.VirtualFileMetadata, error) {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return nil, err
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		return nil, err
	}

	relative := ToRelativePath(absolute, mnt.Path)
	// We need to determine if the file exists
	if mnt.Metadata != nil {
		// Try to read info from metadata
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				return nil, err
			}
		} else {
			return meta, nil
		}
	}
	// Fallback to storage to read object stats
	stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
	if err != nil {
		return nil, err
	}
	// Convert object stats to metadata
	meta := stat.ToMetadata()
	if !mnt.IsDualMount {
		// Write stat back into metadata
		if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
			return nil, err
		}
	}

	return meta, nil
}

// Chmod changes the mode (permissions) of the file at path.
// Returns an error if the operation is not supported or fails.
func (vfs *VirtualFileSystem) ChangeMode(ctx context.Context, path string, mode data.VirtualFileMode) error {
	return fmt.Errorf("vfs: not implemented")
}

// Mount attaches a filesystem handler at the specified path.
// Options can be used to configure the mount (e.g., read-only).
func (vfs *VirtualFileSystem) Mount(ctx context.Context, path string, primary backend.VirtualObjectStorageBackend, opts ...mount.MountOption) error {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	if len(absolute) == 0 {
		return data.ErrInvalidPath
	}
	// Check if parent mount denies nesting BEFORE acquiring write lock
	if parent, err := vfs.getMountFromPath(absolute); err == nil {
		if !parent.Options.Nesting {
			return data.ErrNestingDenied
		}
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	mnt, err := mount.NewMountInfo(path, primary, opts...)
	if err != nil {
		return err
	}

	if _, exists := vfs.mnts[absolute]; exists {
		return data.ErrAlreadyMounted
	}

	if err := mnt.Mount(ctx); err != nil {
		return data.ErrMountFailed
	}

	vfs.mnts[absolute] = mnt
	return nil
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (vfs *VirtualFileSystem) Unmount(ctx context.Context, path string, force bool) error {
	// Always start with an absolute path
	absolute, err := ToAbsolutePath(path)
	if err != nil {
		return err
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	mnt, exists := vfs.mnts[absolute]
	if !exists {
		return data.ErrNotMounted
	}

	if vfs.hasChildMounts(absolute) {
		return data.ErrMountBusy
	}

	if err := mnt.Unmount(ctx, force); err != nil {
		return data.ErrUnmountFailed
	}

	delete(vfs.mnts, absolute)
	return nil
}

// Lookup checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (vfs *VirtualFileSystem) Lookup(ctx context.Context, path string) (bool, error) {
	meta, err := vfs.StatMetadata(ctx, path)
	if err != nil {
		return false, nil
	}

	return (meta != nil), nil
}

func (vfs *VirtualFileSystem) getMountFromPath(path string) (*mount.Mount, error) {
	// Skip, if no entries have been mounted yet
	if len(vfs.mnts) == 0 {
		return nil, data.ErrNotMounted
	}

	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	var best *mount.Mount
	for point, mnt := range vfs.mnts {
		if hasPrefix(path, point) {
			// For root mount ("/"), it matches everything
			// For other mounts, ensure exact match or path continues with /
			if point == "/" || len(path) == len(point) || (len(path) > len(point) && path[len(point)] == '/') {
				if best == nil || len(point) > len(best.Path) {
					best = mnt
				}
			}
		}
	}

	if best == nil {
		return nil, data.ErrNotMounted
	}

	return best, nil
}

func (vfs *VirtualFileSystem) hasChildMounts(parent string) bool {
	for mount := range vfs.mnts {
		if mount != parent && hasPrefix(mount, parent) {
			return true
		}
	}

	return false
}
