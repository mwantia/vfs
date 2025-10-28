package vfs

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mwantia/vfs/backend"
	"github.com/mwantia/vfs/backend/memory"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
	"github.com/mwantia/vfs/log"
	"github.com/mwantia/vfs/mount"
)

// VirtualFileSystem is the main VFS manager that handles mount points and delegates
// file operations to the appropriate mount handlers. It provides a Unix-like filesystem
// abstraction with support for nested mounts and thread-safe operations.
type VirtualFileSystem struct {
	mu   sync.RWMutex
	log  *log.Logger
	mnts map[string]*mount.Mount
}

// NewVfs creates a new VirtualFileSystem instance with no initial mounts.
func NewVfs(opts ...VirtualFileSystemOption) (*VirtualFileSystem, error) {
	options := newDefaultVirtualFileSystemOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	vfs := &VirtualFileSystem{
		log:  log.NewLogger("vfs", options.LogLevel, options.LogFile, options.NoTerminalLog),
		mnts: make(map[string]*mount.Mount),
	}

	vfs.log.Info("VFS initialized with log level: %s", options.LogLevel)

	// Create base root mount if enabled
	if options.BaseRootMount {
		vfs.log.Info("Creating base root mount at /")
		ctx := context.Background()
		root := memory.NewMemoryBackend()
		if err := vfs.Mount(ctx, "/", root, mount.AsReadOnly()); err != nil {
			vfs.log.Error("Failed to create base root mount: %v", err)
			return nil, err
		}
		vfs.log.Info("Base root mount created successfully")
	}

	return vfs, nil
}

// Open opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (vfs *VirtualFileSystem) OpenFile(ctx context.Context, path string, flags data.VirtualAccessMode) (mount.Streamer, error) {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("Failed to convert path to absolute: %s - %v", path, err)
		return nil, err
	}

	vfs.log.Debug("OpenFile: path=%s flags=%v", absolute, flags)

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("No mount found for path: %s - %v", absolute, err)
		return nil, err
	}

	vfs.log.Debug("OpenFile: resolved to mount at %s", mnt.Path)
	relative := data.ToRelativePath(absolute, mnt.Path)
	// Check if any previous streamer exists
	streamer, exists := mnt.GetStreamer(relative)
	if exists {
		vfs.log.Debug("OpenFile: reusing existing streamer for %s", absolute)
		return streamer, nil
	}
	// Fail if we expect to create or write a stream on a readonly mount
	if mnt.Options.ReadOnly {
		if flags&data.AccessModeWrite != 0 || flags&data.AccessModeCreate != 0 || flags&data.AccessModeExcl != 0 {
			vfs.log.Error("OpenFile: cannot write to read-only mount at %s", mnt.Path)
			return nil, data.ErrReadOnly
		}
	}
	// Determine initial offset
	offset := int64(0)
	// We need to determine if the file exists
	if mnt.Metadata != nil {
		vfs.log.Debug("OpenFile: using metadata backend for %s", absolute)
		// Try to read info from metadata
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				vfs.log.Error("OpenFile: metadata read failed for %s - %v", absolute, err)
				return nil, err
			}
			vfs.log.Debug("OpenFile: metadata not found for %s, falling back to object storage", absolute)
			// Fallback to storage to read object stats
			stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
			if err != nil {
				// Fail if any error except NotExists
				if err != data.ErrNotExist {
					vfs.log.Error("OpenFile: object storage HeadObject failed for %s - %v", absolute, err)
					return nil, err
				}
				// Return when the file shouldn't be created
				if !flags.HasCreate() {
					vfs.log.Debug("OpenFile: file %s does not exist and CREATE flag not set", absolute)
					return nil, err
				}
				vfs.log.Info("OpenFile: creating new file %s in object storage", absolute)
				stat, err = mnt.ObjectStorage.CreateObject(ctx, relative, 0x777)
				if err != nil {
					vfs.log.Error("OpenFile: failed to create object in storage for %s - %v", absolute, err)
					return nil, err
				}
			}
			// Convert object stats to metadata
			meta = stat.ToMetadata()
			if !mnt.IsDualMount && mnt.Metadata != nil {
				vfs.log.Debug("OpenFile: syncing object stat to metadata for %s", absolute)
				// Write stat back into metadata
				if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
					vfs.log.Error("OpenFile: failed to sync metadata for %s - %v", absolute, err)
					return nil, err
				}
			}
		} else {
			vfs.log.Debug("OpenFile: found metadata for %s (size=%d)", absolute, meta.Size)
		}
		// File exists - check EXCL flag
		if flags.HasExcl() && flags.HasCreate() {
			vfs.log.Error("OpenFile: file %s already exists (EXCL flag set)", absolute)
			return nil, data.ErrExist
		}
		// Unable to open directories as streams
		if meta.Mode.IsDir() {
			vfs.log.Error("OpenFile: cannot open directory %s as stream", absolute)
			return nil, data.ErrIsDirectory
		}
		// Read filesize from metadata if append
		if flags.HasAppend() {
			offset = meta.Size
			vfs.log.Debug("OpenFile: APPEND mode, setting offset to %d for %s", offset, absolute)
		}
	} else {
		vfs.log.Debug("OpenFile: using object storage directly (no metadata backend) for %s", absolute)
		// Fallback to storage to read object stats
		stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				vfs.log.Error("OpenFile: object storage HeadObject failed for %s - %v", absolute, err)
				return nil, err
			}
			// Return when we file shouldn't be created
			if !flags.HasCreate() {
				vfs.log.Debug("OpenFile: file %s does not exist and CREATE flag not set", absolute)
				return nil, err
			}
			vfs.log.Info("OpenFile: creating new file %s in object storage", absolute)
			stat, err = mnt.ObjectStorage.CreateObject(ctx, relative, 0x777)
			if err != nil {
				vfs.log.Error("OpenFile: failed to create object in storage for %s - %v", absolute, err)
				return nil, err
			}
		}
		// File exists - check EXCL flag
		if flags.HasExcl() && flags.HasCreate() {
			vfs.log.Error("OpenFile: file %s already exists (EXCL flag set)", absolute)
			return nil, data.ErrExist
		}
		// Unable to open directories as streams
		if stat.Mode.IsDir() {
			vfs.log.Error("OpenFile: cannot open directory %s as stream", absolute)
			return nil, data.ErrIsDirectory
		}
		// Read filesize from metadata if append
		if flags.HasAppend() {
			offset = stat.Size
			vfs.log.Debug("OpenFile: APPEND mode, setting offset to %d for %s", offset, absolute)
		}
	}
	// Only truncate if TRUNC flag is set and we have write access
	if flags.HasTrunc() && (flags.IsWriteOnly() || flags.IsReadWrite()) {
		vfs.log.Debug("OpenFile: truncating file %s", absolute)
		if err := mnt.ObjectStorage.TruncateObject(ctx, relative, 0); err != nil {
			vfs.log.Error("OpenFile: failed to truncate file %s - %v", absolute, err)
			return nil, err
		}
	}

	vfs.log.Info("OpenFile: successfully opened %s with offset=%d", absolute, offset)
	return mnt.OpenStreamer(ctx, relative, offset, flags), nil
}

// Close closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (vfs *VirtualFileSystem) CloseFile(ctx context.Context, path string, force bool) error {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("CloseFile: failed to convert path to absolute: %s - %v", path, err)
		return err
	}

	vfs.log.Debug("CloseFile: path=%s force=%v", absolute, force)

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("CloseFile: no mount found for path: %s - %v", absolute, err)
		return err
	}

	relative := data.ToRelativePath(absolute, mnt.Path)
	if err := mnt.CloseStreamer(ctx, relative, force); err != nil {
		vfs.log.Error("CloseFile: failed to close streamer for %s - %v", absolute, err)
		return err
	}

	vfs.log.Info("CloseFile: successfully closed %s", absolute)
	return nil
}

// Read reads size bytes from the file at path starting at offset.
// Returns the data read or an error if the operation fails.
func (vfs *VirtualFileSystem) ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error) {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("ReadFile: failed to convert path to absolute: %s - %v", path, err)
		return nil, err
	}
	// Fail immediately, if we receive an invalid size
	if size <= 0 {
		vfs.log.Error("ReadFile: invalid size %d for path %s", size, absolute)
		return nil, fmt.Errorf("vfs: size out of range")
	}

	vfs.log.Debug("ReadFile: path=%s offset=%d size=%d", absolute, offset, size)

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("ReadFile: no mount found for path: %s - %v", absolute, err)
		return nil, err
	}

	relative := data.ToRelativePath(absolute, mnt.Path)
	// If metadata exists, validate if size and offset matches
	if mnt.Metadata != nil {
		vfs.log.Debug("ReadFile: validating size using metadata for %s", absolute)
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				vfs.log.Error("ReadFile: metadata read failed for %s - %v", absolute, err)
				return nil, err
			}
			vfs.log.Debug("ReadFile: metadata not found, falling back to object storage for %s", absolute)
			// Fallback to storage to read object stats
			stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
			if err != nil {
				vfs.log.Error("ReadFile: object storage HeadObject failed for %s - %v", absolute, err)
				return nil, err
			}
			// Convert object stats to metadata
			meta = stat.ToMetadata()
			if !mnt.IsDualMount && mnt.Metadata != nil {
				vfs.log.Debug("ReadFile: syncing object stat to metadata for %s", absolute)
				// Write stat back into metadata
				if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
					vfs.log.Warn("ReadFile: failed to sync metadata for %s - %v", absolute, err)
					return nil, err
				}
			}
		}
		// Ignore files, which cannot be read
		if meta.Mode.IsDir() {
			vfs.log.Error("ReadFile: cannot read directory %s", absolute)
			return nil, data.ErrIsDirectory
		}
		// Validate filesize
		if (offset + size) > meta.Size {
			vfs.log.Error("ReadFile: read range exceeds file size for %s (offset=%d size=%d filesize=%d)", absolute, offset, size, meta.Size)
			return nil, fmt.Errorf("vfs: size out of range")
		}
	} else {
		vfs.log.Debug("ReadFile: validating size using object storage for %s", absolute)
		// Fallback to storage to read object stats
		stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			vfs.log.Error("ReadFile: object storage HeadObject failed for %s - %v", absolute, err)
			return nil, err
		}
		// Ignore files, which cannot be read
		if stat.Mode.IsDir() {
			vfs.log.Error("ReadFile: cannot read directory %s", absolute)
			return nil, data.ErrIsDirectory
		}
		// Validate filesize
		if (offset + size) > stat.Size {
			vfs.log.Error("ReadFile: read range exceeds file size for %s (offset=%d size=%d filesize=%d)", absolute, offset, size, stat.Size)
			return nil, fmt.Errorf("vfs: size out of range")
		}
	}

	vfs.log.Debug("ReadFile: reading from object storage for %s", absolute)
	buffer := make([]byte, size)
	n, err := mnt.ObjectStorage.ReadObject(ctx, relative, offset, buffer)
	if err != nil && err != io.EOF {
		vfs.log.Error("ReadFile: object storage ReadObject failed for %s - %v", absolute, err)
		return nil, err
	}

	vfs.log.Debug("ReadFile: successfully read %d bytes from %s", n, absolute)
	return buffer[:n], nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (vfs *VirtualFileSystem) WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error) {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("WriteFile: failed to convert path to absolute: %s - %v", path, err)
		return 0, err
	}

	vfs.log.Debug("WriteFile: path=%s offset=%d size=%d", absolute, offset, len(buffer))

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("WriteFile: no mount found for path: %s - %v", absolute, err)
		return 0, err
	}
	// Fail if mount is readonly
	if mnt.Options.ReadOnly {
		vfs.log.Error("WriteFile: cannot write to read-only mount at %s", mnt.Path)
		return 0, data.ErrReadOnly
	}

	relative := data.ToRelativePath(absolute, mnt.Path)
	// Read metadata info if available
	if mnt.Metadata != nil {
		vfs.log.Debug("WriteFile: validating file using metadata for %s", absolute)
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				vfs.log.Error("WriteFile: metadata read failed for %s - %v", absolute, err)
				return 0, err
			}
			vfs.log.Debug("WriteFile: metadata not found, falling back to object storage for %s", absolute)
			// Fallback to storage to read object stats
			stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
			if err != nil {
				vfs.log.Error("WriteFile: object storage HeadObject failed for %s - %v", absolute, err)
				return 0, err
			}
			// Convert object stats to metadata
			meta = stat.ToMetadata()
			if !mnt.IsDualMount && mnt.Metadata != nil {
				vfs.log.Debug("WriteFile: syncing object stat to metadata for %s", absolute)
				// Write stat back into metadata
				if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
					vfs.log.Error("WriteFile: failed to sync metadata for %s - %v", absolute, err)
					return 0, err
				}
			}
		}
		// Ignore files, which cannot be read
		if meta.Mode.IsDir() {
			vfs.log.Error("WriteFile: cannot write to directory %s", absolute)
			return 0, data.ErrIsDirectory
		}
	} else {
		vfs.log.Debug("WriteFile: validating file using object storage for %s", absolute)
		// Fallback to storage to read object stats
		stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			vfs.log.Error("WriteFile: object storage HeadObject failed for %s - %v", absolute, err)
			return 0, err
		}
		// Ignore files, which cannot be read
		if stat.Mode.IsDir() {
			vfs.log.Error("WriteFile: cannot write to directory %s", absolute)
			return 0, data.ErrIsDirectory
		}
	}

	vfs.log.Debug("WriteFile: writing to object storage for %s", absolute)
	n, err := mnt.ObjectStorage.WriteObject(ctx, relative, offset, buffer)
	if err != nil {
		vfs.log.Error("WriteFile: object storage WriteObject failed for %s - %v", absolute, err)
		return 0, err
	}

	// Sync metadata information after successfull write
	if mnt.Metadata != nil && !mnt.IsDualMount {
		vfs.log.Debug("WriteFile: syncing updated size to metadata for %s (new_size=%d)", absolute, offset+int64(n))
		update := &data.VirtualFileMetadataUpdate{
			Mask: data.VirtualFileMetadataUpdateSize,
			Metadata: &data.VirtualFileMetadata{
				Size: offset + int64(n),
			},
		}
		if err := mnt.Metadata.UpdateMeta(ctx, relative, update); err != nil {
			vfs.log.Warn("WriteFile: failed to update metadata for %s - %v", absolute, err)
			return n, err
		}
	}

	vfs.log.Info("WriteFile: successfully wrote %d bytes to %s at offset %d", n, absolute, offset)
	return n, err
}

// CreateDirectory creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (vfs *VirtualFileSystem) CreateDirectory(ctx context.Context, path string) error {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("CreateDirectory: failed to convert path to absolute: %s - %v", path, err)
		return err
	}

	vfs.log.Debug("CreateDirectory: path=%s", absolute)

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("CreateDirectory: no mount found for path: %s - %v", absolute, err)
		return err
	}
	// Fail if mount is readonly
	if mnt.Options.ReadOnly {
		vfs.log.Error("CreateDirectory: cannot create directory on read-only mount at %s", mnt.Path)
		return data.ErrReadOnly
	}

	relative := data.ToRelativePath(absolute, mnt.Path)
	// Check if path exists in metadata
	if mnt.Metadata != nil {
		if exists, _ := mnt.Metadata.ExistsMeta(ctx, relative); exists {
			vfs.log.Error("CreateDirectory: directory %s already exists in metadata", absolute)
			return data.ErrExist
		}
	}

	// Create folder in object storage
	vfs.log.Debug("CreateDirectory: creating directory in object storage for %s", absolute)
	stat, err := mnt.ObjectStorage.CreateObject(ctx, relative, data.ModeDir|0x777)
	if err != nil {
		// Fail if any error except Exists
		if err != data.ErrExist {
			vfs.log.Error("CreateDirectory: failed to create directory in object storage for %s - %v", absolute, err)
			return err
		}
	}
	// Write back into metadata (only if separate metadata backend)
	if mnt.Metadata != nil && !mnt.IsDualMount {
		vfs.log.Debug("CreateDirectory: syncing directory to metadata for %s", absolute)
		// In case stat hasn't been provided from 'CreateObject'
		if stat == nil {
			stat, err = mnt.ObjectStorage.HeadObject(ctx, relative)
			if err != nil {
				vfs.log.Error("CreateDirectory: failed to read directory stat for %s - %v", absolute, err)
				return err
			}
		}

		meta := stat.ToMetadata()
		if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
			vfs.log.Error("CreateDirectory: failed to sync directory metadata for %s - %v", absolute, err)
			return err
		}
	}

	vfs.log.Info("CreateDirectory: successfully created directory %s", absolute)
	return nil
}

// RemoveDirectory removes an empty directory at the specified path.
// Returns an error if the directory is not empty or doesn't exist.
func (vfs *VirtualFileSystem) RemoveDirectory(ctx context.Context, path string, force bool) error {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("RemoveDirectory: failed to convert path to absolute: %s - %v", path, err)
		return err
	}

	vfs.log.Debug("RemoveDirectory: path=%s force=%v", absolute, force)

	// Prevent deletion of mount points
	vfs.mu.RLock()
	_, isMountPoint := vfs.mnts[absolute]
	vfs.mu.RUnlock()

	if isMountPoint {
		vfs.log.Error("RemoveDirectory: cannot remove mount point %s", absolute)
		return errors.PathMountBusy(nil, absolute)
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("RemoveDirectory: no mount found for path: %s - %v", absolute, err)
		return err
	}
	// Fail if mount is readonly
	if mnt.Options.ReadOnly {
		vfs.log.Error("RemoveDirectory: cannot remove directory on read-only mount at %s", mnt.Path)
		return data.ErrReadOnly
	}

	relative := data.ToRelativePath(absolute, mnt.Path)

	var stat *data.VirtualFileStat
	// Check if path exists in metadata
	if mnt.Metadata != nil {
		vfs.log.Debug("RemoveDirectory: checking directory existence using metadata for %s", absolute)
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			vfs.log.Error("RemoveDirectory: metadata read failed for %s - %v", absolute, err)
			return err
		}

		stat = meta.ToStat()
		vfs.log.Debug("RemoveDirectory: found directory in metadata (mode=%s)", stat.Mode)
	} else {
		vfs.log.Debug("RemoveDirectory: checking directory existence using object storage for %s", absolute)
		stat, err = mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			vfs.log.Error("RemoveDirectory: object storage HeadObject failed for %s - %v", absolute, err)
			return err
		}
		vfs.log.Debug("RemoveDirectory: found directory in object storage (mode=%s)", stat.Mode)
	}
	// RemoveDirectory is only supposed to delete directories
	if !stat.Mode.IsDir() {
		vfs.log.Error("RemoveDirectory: path %s is not a directory (mode=%s)", absolute, stat.Mode)
		return data.ErrNotDirectory
	}

	if !force {
		vfs.log.Debug("RemoveDirectory: validating directory is empty for %s", absolute)
		if mnt.Metadata != nil {
			query := &backend.MetadataQuery{
				Prefix:    relative + "/",
				Delimiter: "/",
				Limit:     1, // Only need to know if ANY children exist
			}

			result, err := mnt.Metadata.QueryMeta(ctx, query)
			if err != nil {
				vfs.log.Error("RemoveDirectory: metadata query failed for %s - %v", absolute, err)
				return err
			}

			if result != nil && result.TotalCount > 0 {
				vfs.log.Error("RemoveDirectory: directory %s is not empty (contains %d entries)", absolute, result.TotalCount)
				return data.ErrDirectoryNotEmpty
			}
			vfs.log.Debug("RemoveDirectory: directory %s is empty (validated via metadata)", absolute)
		} else {
			// Check object storage to see if directory is empty
			entries, err := mnt.ObjectStorage.ListObjects(ctx, relative)
			if err != nil {
				vfs.log.Error("RemoveDirectory: object storage ListObjects failed for %s - %v", absolute, err)
				return err
			}
			// Directory should only contain itself (or be completely empty)
			if len(entries) > 1 || (len(entries) == 1 && entries[0].Key != relative) {
				vfs.log.Error("RemoveDirectory: directory %s is not empty (contains %d entries)", absolute, len(entries))
				return data.ErrDirectoryNotEmpty
			}
			vfs.log.Debug("RemoveDirectory: directory %s is empty (validated via object storage)", absolute)
		}
	} else {
		vfs.log.Debug("RemoveDirectory: force flag set, skipping empty directory check for %s", absolute)
	}

	// Delete directory from object storage
	// Force to specifically delete directories
	vfs.log.Debug("RemoveDirectory: deleting directory from object storage for %s", absolute)
	if err := mnt.ObjectStorage.DeleteObject(ctx, relative, true); err != nil {
		vfs.log.Error("RemoveDirectory: failed to delete directory from object storage for %s - %v", absolute, err)
		return err
	}
	// Sync deletion to metadata if available
	if mnt.Metadata != nil && !mnt.IsDualMount {
		vfs.log.Debug("RemoveDirectory: syncing deletion to metadata for %s", absolute)
		// TODO :: For directories we need to delete all child metadata
		if err := mnt.Metadata.DeleteMeta(ctx, relative); err != nil {
			vfs.log.Error("RemoveDirectory: failed to delete metadata for %s - %v", absolute, err)
			return err
		}
	}

	vfs.log.Info("RemoveDirectory: successfully removed directory %s", absolute)
	return nil
}

// UnlinkFile removes a file at the specified path.
// Returns an error if the path is a directory or doesn't exist.
func (vfs *VirtualFileSystem) UnlinkFile(ctx context.Context, path string) error {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("UnlinkFile: failed to convert path to absolute: %s - %v", path, err)
		return err
	}

	vfs.log.Debug("UnlinkFile: path=%s", absolute)

	// Prevent deletion of mount points
	vfs.mu.RLock()
	_, isMountPoint := vfs.mnts[absolute]
	vfs.mu.RUnlock()

	if isMountPoint {
		vfs.log.Error("UnlinkFile: cannot unlink mount point %s", absolute)
		return errors.PathMountBusy(nil, absolute)
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("UnlinkFile: no mount found for path: %s - %v", absolute, err)
		return err
	}
	// Fail if mount is readonly
	if mnt.Options.ReadOnly {
		vfs.log.Error("UnlinkFile: cannot unlink file on read-only mount at %s", mnt.Path)
		return data.ErrReadOnly
	}

	relative := data.ToRelativePath(absolute, mnt.Path)

	var stat *data.VirtualFileStat
	// Check if path exists in metadata
	if mnt.Metadata != nil {
		vfs.log.Debug("UnlinkFile: checking file existence using metadata for %s", absolute)
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			vfs.log.Error("UnlinkFile: metadata read failed for %s - %v", absolute, err)
			return err
		}

		stat = meta.ToStat()
		vfs.log.Debug("UnlinkFile: found file in metadata (size=%d mode=%s)", stat.Size, stat.Mode)
	} else {
		vfs.log.Debug("UnlinkFile: checking file existence using object storage for %s", absolute)
		stat, err = mnt.ObjectStorage.HeadObject(ctx, relative)
		if err != nil {
			vfs.log.Error("UnlinkFile: object storage HeadObject failed for %s - %v", absolute, err)
			return err
		}
		vfs.log.Debug("UnlinkFile: found file in object storage (size=%d mode=%s)", stat.Size, stat.Mode)
	}
	// Unlink is unable to delete directories
	if stat.Mode.IsDir() {
		vfs.log.Error("UnlinkFile: cannot unlink directory %s (use RemoveDirectory instead)", absolute)
		return data.ErrIsDirectory
	}
	// Delete file from object storage
	// Force to specifically deletes directories
	vfs.log.Debug("UnlinkFile: deleting file from object storage for %s", absolute)
	if err := mnt.ObjectStorage.DeleteObject(ctx, relative, false); err != nil {
		vfs.log.Error("UnlinkFile: failed to delete file from object storage for %s - %v", absolute, err)
		return err
	}
	// Sync deletion to metadata if available
	if mnt.Metadata != nil && !mnt.IsDualMount {
		vfs.log.Debug("UnlinkFile: syncing deletion to metadata for %s", absolute)
		if err := mnt.Metadata.DeleteMeta(ctx, relative); err != nil {
			vfs.log.Error("UnlinkFile: failed to delete metadata for %s - %v", absolute, err)
			return err
		}
	}

	vfs.log.Info("UnlinkFile: successfully unlinked file %s", absolute)
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
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("ReadDirectory: failed to convert path to absolute: %s - %v", path, err)
		return nil, err
	}

	vfs.log.Debug("ReadDirectory: path=%s", absolute)

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("ReadDirectory: no mount found for path: %s - %v", absolute, err)
		return nil, err
	}

	relative := data.ToRelativePath(absolute, mnt.Path)

	// Use a map to track entries by key to avoid duplicates
	metaMap := make(map[string]*data.VirtualFileMetadata)

	// Try to get entries from metadata first if available
	foundInMetadata := false
	if mnt.Metadata != nil {
		vfs.log.Debug("ReadDirectory: attempting to read from metadata backend for %s", absolute)
		// Calculate the correct prefix for querying
		prefix := relative
		if prefix != "" {
			prefix += "/"
		}

		query := &backend.MetadataQuery{
			Prefix:    prefix,
			Delimiter: "/", // Only return direct children, not entire tree
			SortBy:    backend.SortByKey,
			SortOrder: backend.SortAsc,
		}

		result, err := mnt.Metadata.QueryMeta(ctx, query)
		if err != nil {
			vfs.log.Error("ReadDirectory: metadata query failed for %s - %v", absolute, err)
			return nil, err
		}

		if result != nil && result.TotalCount > 0 {
			vfs.log.Debug("ReadDirectory: found %d entries in metadata for %s", result.TotalCount, absolute)
			// Add all metadata entries to the map
			// Strip the directory prefix from keys to make them relative
			for _, meta := range result.Candidates {
				// Clone metadata and adjust key to be relative to current directory
				relativeMeta := meta.Clone()
				if prefix != "" {
					relativeMeta.Key = strings.TrimPrefix(meta.Key, prefix)
				}
				metaMap[relativeMeta.Key] = relativeMeta
			}
			foundInMetadata = true
		} else {
			vfs.log.Debug("ReadDirectory: no entries found in metadata for %s, will check object storage", absolute)
		}
	}

	// If not found in metadata, or no metadata backend, fall back to storage
	if !foundInMetadata {
		vfs.log.Debug("ReadDirectory: reading from object storage backend for %s", absolute)
		// Use storage backend to list objects
		stats, err := mnt.ObjectStorage.ListObjects(ctx, relative)
		if err != nil {
			// If directory doesn't exist but we're at a mount point root, that's OK - just empty
			if err == data.ErrNotExist && relative == "" {
				vfs.log.Debug("ReadDirectory: empty mount root for %s, continuing with empty list", absolute)
				// This is the root of a mount with no entries yet - continue with empty list
			} else {
				vfs.log.Error("ReadDirectory: object storage ListObjects failed for %s - %v", absolute, err)
				return nil, err
			}
		} else {
			vfs.log.Debug("ReadDirectory: found %d entries in object storage for %s", len(stats), absolute)
			// Convert stats to metadata
			// Calculate prefix for stripping (to make keys relative to current directory for display)
			prefix := relative
			if prefix != "" {
				prefix += "/"
			}

			for _, stat := range stats {
				meta := stat.ToMetadata()

				// The stat.Key from storage is relative to the listed directory
				// We need to prepend the directory path to get the full mount-relative key for metadata
				fullKey := stat.Key
				if relative != "" {
					fullKey = relative + "/" + stat.Key
				}

				// Sync to metadata if available AND it's a separate backend instance
				if mnt.Metadata != nil && !mnt.IsDualMount {
					// Create a copy with the full mount-relative key for metadata storage
					metaForSync := meta.Clone()
					metaForSync.Key = fullKey
					vfs.log.Debug("ReadDirectory: syncing entry %s to metadata", metaForSync.Key)
					if err := mnt.Metadata.CreateMeta(ctx, metaForSync); err != nil {
						vfs.log.Warn("ReadDirectory: failed to sync metadata for %s - %v (continuing)", metaForSync.Key, err)
						continue // Ignore errors for existing metadata
					}
				}

				// Use the relative key (just the name) for the directory listing result
				relativeKey := stat.Key
				relativeMeta := meta.Clone()
				relativeMeta.Key = relativeKey
				metaMap[relativeKey] = relativeMeta
			}
		}
	}

	// Inject virtual mount point entries
	childMounts := vfs.getChildMounts(absolute)
	if len(childMounts) > 0 {
		vfs.log.Debug("ReadDirectory: injecting %d virtual mount points for %s", len(childMounts), absolute)
	}
	for _, mountPath := range childMounts {
		// Extract the mount point name (last component of the path)
		mountName := mountPath
		if lastSlash := strings.LastIndex(mountPath, "/"); lastSlash >= 0 {
			mountName = mountPath[lastSlash+1:]
		}

		// Calculate the relative key for this mount point
		mountKey := relative
		if mountKey != "" {
			mountKey = mountKey + "/" + mountName
		} else {
			mountKey = mountName
		}

		// Only add if not already present (real directory takes precedence)
		if _, exists := metaMap[mountKey]; !exists {
			vfs.log.Debug("ReadDirectory: adding virtual mount point %s at %s", mountName, mountPath)
			// Create virtual metadata for the mount point
			now := time.Now()
			metaMap[mountKey] = &data.VirtualFileMetadata{
				ID:          mountPath, // Use mount path as ID
				Key:         mountKey,
				Mode:        data.ModeMount | data.ModeDir | 0755,
				Size:        0,
				AccessTime:  now,
				ModifyTime:  now,
				CreateTime:  now,
				ContentType: "inode/directory",
			}
		} else {
			vfs.log.Debug("ReadDirectory: mount point %s already exists as real directory", mountName)
		}
	}

	// Convert map back to slice and sort by key
	metas := make([]*data.VirtualFileMetadata, 0, len(metaMap))
	for _, meta := range metaMap {
		metas = append(metas, meta)
	}

	// Sort entries by key for consistent ordering
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Key < metas[j].Key
	})

	vfs.log.Info("ReadDirectory: successfully read %d entries from %s", len(metas), absolute)
	return metas, nil
}

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (vfs *VirtualFileSystem) StatMetadata(ctx context.Context, path string) (*data.VirtualFileMetadata, error) {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("StatMetadata: failed to convert path to absolute: %s - %v", path, err)
		return nil, err
	}

	vfs.log.Debug("StatMetadata: path=%s", absolute)

	// Check if this path is itself a mount point
	vfs.mu.RLock()
	_, isMountPoint := vfs.mnts[absolute]
	vfs.mu.RUnlock()

	if isMountPoint {
		vfs.log.Debug("StatMetadata: path %s is a mount point, returning virtual metadata", absolute)
		// Return virtual metadata for the mount point itself
		now := time.Now()
		// Extract the mount point name (last component of the path)
		mountName := absolute
		if lastSlash := strings.LastIndex(absolute, "/"); lastSlash >= 0 && lastSlash < len(absolute)-1 {
			mountName = absolute[lastSlash+1:]
		} else if absolute == "/" {
			mountName = ""
		}

		return &data.VirtualFileMetadata{
			ID:          absolute,
			Key:         mountName,
			Mode:        data.ModeMount | data.ModeDir | 0755,
			Size:        0,
			AccessTime:  now,
			ModifyTime:  now,
			CreateTime:  now,
			ContentType: "inode/directory",
		}, nil
	}

	mnt, err := vfs.getMountFromPath(absolute)
	if err != nil {
		vfs.log.Error("StatMetadata: no mount found for path: %s - %v", absolute, err)
		return nil, err
	}

	relative := data.ToRelativePath(absolute, mnt.Path)
	// We need to determine if the file exists
	if mnt.Metadata != nil {
		vfs.log.Debug("StatMetadata: querying metadata backend for %s", absolute)
		// Try to read info from metadata
		meta, err := mnt.Metadata.ReadMeta(ctx, relative)
		if err != nil {
			// Fail if any error except NotExists
			if err != data.ErrNotExist {
				vfs.log.Error("StatMetadata: metadata read failed for %s - %v", absolute, err)
				return nil, err
			}
			vfs.log.Debug("StatMetadata: metadata not found for %s, falling back to object storage", absolute)
		} else {
			vfs.log.Debug("StatMetadata: found in metadata (size=%d mode=%s)", meta.Size, meta.Mode)
			return meta, nil
		}
	} else {
		vfs.log.Debug("StatMetadata: no metadata backend, using object storage for %s", absolute)
	}
	// Fallback to storage to read object stats
	vfs.log.Debug("StatMetadata: querying object storage backend for %s", absolute)
	stat, err := mnt.ObjectStorage.HeadObject(ctx, relative)
	if err != nil {
		vfs.log.Error("StatMetadata: object storage HeadObject failed for %s - %v", absolute, err)
		return nil, err
	}
	vfs.log.Debug("StatMetadata: found in object storage (size=%d mode=%s)", stat.Size, stat.Mode)
	// Convert object stats to metadata
	meta := stat.ToMetadata()
	if !mnt.IsDualMount && mnt.Metadata != nil {
		vfs.log.Debug("StatMetadata: syncing object stat to metadata for %s", absolute)
		// Write stat back into metadata
		if err := mnt.Metadata.CreateMeta(ctx, meta); err != nil {
			vfs.log.Warn("StatMetadata: failed to sync metadata for %s - %v", absolute, err)
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
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("Mount: failed to convert path to absolute: %s - %v", path, err)
		return err
	}

	vfs.log.Debug("Mount: mounting %s backend at %s", primary.Name(), absolute)

	if len(absolute) == 0 {
		vfs.log.Error("Mount: invalid empty path")
		return errors.InvalidPath(nil, absolute)
	}
	// Check if parent mount denies nesting BEFORE acquiring write lock
	if parent, err := vfs.getMountFromPath(absolute); err == nil {
		if !parent.Options.Nesting {
			vfs.log.Error("Mount: parent mount at %s denies nesting", parent.Path)
			return errors.PathMountNestingDenied(nil, parent.Path)
		}
		vfs.log.Debug("Mount: parent mount at %s allows nesting", parent.Path)
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	name := fmt.Sprintf("mount/%s", primary.Name())
	log := vfs.log.Named(name)

	mnt, err := mount.NewMountInfo(path, log, primary, opts...)
	if err != nil {
		vfs.log.Error("Mount: failed to create mount info for %s - %v", absolute, err)
		return err
	}

	if _, exists := vfs.mnts[absolute]; exists {
		vfs.log.Error("Mount: path %s is already mounted", absolute)
		return errors.PathAlreadyMounted(nil, absolute)
	}

	vfs.log.Debug("Mount: initializing mount at %s (readonly=%v dual=%v)", absolute, mnt.Options.ReadOnly, mnt.IsDualMount)
	if err := mnt.Mount(ctx); err != nil {
		vfs.log.Error("Mount: failed to mount %s backend at %s - %v", primary.Name(), absolute, err)
		return data.ErrMountFailed
	}

	vfs.mnts[absolute] = mnt
	vfs.log.Info("Mount: successfully mounted %s at %s", primary.Name(), absolute)
	return nil
}

// Unmount removes the filesystem handler at the specified path.
// Returns an error if the path is not mounted or has child mounts.
func (vfs *VirtualFileSystem) Unmount(ctx context.Context, path string, force bool) error {
	// Always start with an absolute path
	absolute, err := data.ToAbsolutePath(path)
	if err != nil {
		vfs.log.Error("Unmount: failed to convert path to absolute: %s - %v", path, err)
		return err
	}

	vfs.log.Debug("Unmount: unmounting %s (force=%v)", absolute, force)

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	mnt, exists := vfs.mnts[absolute]
	if !exists {
		vfs.log.Error("Unmount: path %s is not mounted", absolute)
		return errors.PathNotMounted(nil, absolute)
	}

	if vfs.hasChildMounts(absolute) {
		vfs.log.Error("Unmount: path %s has child mounts, cannot unmount", absolute)
		return errors.PathMountBusy(nil, absolute)
	}

	vfs.log.Debug("Unmount: closing mount at %s", absolute)
	if err := mnt.Unmount(ctx, force); err != nil {
		vfs.log.Error("Unmount: failed to unmount %s - %v", absolute, err)
		return data.ErrUnmountFailed
	}

	delete(vfs.mnts, absolute)
	vfs.log.Info("Unmount: successfully unmounted %s", absolute)
	return nil
}

// Close unmounts all mounted filesystems and releases all resources.
// This should be called when shutting down the VFS to ensure proper cleanup.
// Mounts are unmounted in reverse order (deepest first) to avoid dependency issues.
func (vfs *VirtualFileSystem) Close(ctx context.Context) error {
	vfs.log.Info("Close: shutting down VFS, unmounting all mounts")

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	// Collect all mount paths and sort them by depth (deepest first)
	var paths []string
	for path := range vfs.mnts {
		paths = append(paths, path)
	}

	vfs.log.Debug("Close: found %d mount(s) to unmount", len(paths))

	// Sort by length (longer paths are deeper in the tree)
	for i := 0; i < len(paths); i++ {
		for j := i + 1; j < len(paths); j++ {
			if len(paths[j]) > len(paths[i]) {
				paths[i], paths[j] = paths[j], paths[i]
			}
		}
	}

	// Unmount all filesystems
	var lastErr error
	unmountedCount := 0
	failedCount := 0
	for _, path := range paths {
		if mnt, exists := vfs.mnts[path]; exists {
			vfs.log.Debug("Close: unmounting %s", path)
			if err := mnt.Unmount(ctx, true); err != nil {
				vfs.log.Error("Close: failed to unmount %s - %v", path, err)
				lastErr = err
				failedCount++
				// Continue trying to unmount others even if one fails
			} else {
				vfs.log.Debug("Close: successfully unmounted %s", path)
				unmountedCount++
			}
			delete(vfs.mnts, path)
		}
	}

	if failedCount > 0 {
		vfs.log.Warn("Close: VFS closed with %d successful and %d failed unmounts", unmountedCount, failedCount)
	} else {
		vfs.log.Info("Close: VFS closed successfully, unmounted %d mount(s)", unmountedCount)
	}

	return lastErr
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
		return nil, errors.PathNotMounted(nil, path)
	}

	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	var best *mount.Mount
	for point, mnt := range vfs.mnts {
		if data.HasPrefix(path, point) {
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
		return nil, errors.PathNotMounted(nil, path)
	}

	return best, nil
}

func (vfs *VirtualFileSystem) hasChildMounts(parent string) bool {
	for mount := range vfs.mnts {
		if mount != parent && data.HasPrefix(mount, parent) {
			return true
		}
	}

	return false
}

// getChildMounts returns all direct child mount points under the given path.
// For example, if path is "/" and mounts exist at "/data" and "/data/cache",
// only "/data" is returned (not "/data/cache" as it's a grandchild).
func (vfs *VirtualFileSystem) getChildMounts(path string) []string {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	children := make([]string, 0)

	// Normalize path - ensure it ends with / for prefix matching
	normalizedPath := path
	if path != "/" && !data.HasPrefix(normalizedPath, "/") {
		normalizedPath = "/" + normalizedPath
	}
	if path != "/" {
		normalizedPath = normalizedPath + "/"
	}

	for mountPath := range vfs.mnts {
		// Skip the path itself
		if mountPath == path {
			continue
		}

		// Skip if not a child of this path
		if !data.HasPrefix(mountPath, normalizedPath) {
			continue
		}

		// Get relative path after the parent
		relative := mountPath
		if path == "/" {
			relative = mountPath[1:] // Remove leading /
		} else {
			relative = mountPath[len(normalizedPath):]
		}

		// Only include direct children (no / in the relative path)
		if relative != "" && !strings.Contains(relative, "/") {
			children = append(children, mountPath)
		}
	}

	return children
}
