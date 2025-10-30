package vfs

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/backend"
)

// OpenFile opens a file with the specified access mode flags and returns a file handle.
// The returned VirtualFile must be closed by the caller. Use flags to control access.
func (vfs *virtualFileSystemImpl) OpenFile(ctx context.Context, path string, flags data.AccessMode) (mount.Streamer, error) {
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

// CloseFile closes an open file handle at the given path.
// This may be a no-op for implementations that don't maintain file handles.
func (vfs *virtualFileSystemImpl) CloseFile(ctx context.Context, path string, force bool) error {
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
func (vfs *virtualFileSystemImpl) ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error) {
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

// validateObjectSize checks if the object size is within the backend's allowed limits.
// Returns an error if the size violates MinObjectSize or MaxObjectSize constraints.
func (vfs *virtualFileSystemImpl) validateObjectSize(mnt *mount.Mount, size int64) error {
	caps := mnt.ObjectStorage.GetCapabilities()

	// Check minimum size if set (0 means no minimum)
	if caps.MinObjectSize > 0 && size < caps.MinObjectSize {
		vfs.log.Error("validateObjectSize: object size %d bytes is below minimum %d bytes", size, caps.MinObjectSize)
		return errors.BackendObjectTooSmall(nil, size, caps.MinObjectSize)
	}

	// Check maximum size if set (0 means no maximum)
	if caps.MaxObjectSize > 0 && size > caps.MaxObjectSize {
		vfs.log.Error("validateObjectSize: object size %d bytes exceeds maximum %d bytes", size, caps.MaxObjectSize)
		return errors.BackendObjectTooLarge(nil, size, caps.MaxObjectSize)
	}

	return nil
}

// Write writes data to the file at path starting at offset.
// Returns the number of bytes written or an error if the operation fails.
func (vfs *virtualFileSystemImpl) WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error) {
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

	var currentSize int64
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
		currentSize = meta.Size
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
		currentSize = stat.Size
	}

	// Calculate the final size after this write
	newSize := offset + int64(len(buffer))
	if currentSize > newSize {
		// If writing in the middle of a file, the size doesn't change
		newSize = currentSize
	}

	// Validate the size against backend capabilities
	vfs.log.Debug("WriteFile: validating size (current=%d new=%d) for %s", currentSize, newSize, absolute)
	if err := vfs.validateObjectSize(mnt, newSize); err != nil {
		vfs.log.Error("WriteFile: size validation failed for %s - %v", absolute, err)
		return 0, err
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
		update := &data.MetadataUpdate{
			Mask: data.MetadataUpdateSize,
			Metadata: &data.Metadata{
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

// Stat returns file information for the given path.
// Returns an error if the path doesn't exist.
func (vfs *virtualFileSystemImpl) StatMetadata(ctx context.Context, path string) (*data.Metadata, error) {
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

		return &data.Metadata{
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

// Lookup checks if a file or directory exists at the given path.
// Returns true if the path exists, false otherwise.
func (vfs *virtualFileSystemImpl) LookupMetadata(ctx context.Context, path string) (bool, error) {
	meta, err := vfs.StatMetadata(ctx, path)
	if err != nil {
		return false, nil
	}

	return (meta != nil), nil
}

// ReadDirectory returns a list of entries in the directory at path.
// Returns an error if the path is not a directory or doesn't exist.
func (vfs *virtualFileSystemImpl) ReadDirectory(ctx context.Context, path string) ([]*data.Metadata, error) {
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
	metaMap := make(map[string]*data.Metadata)

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
			metaMap[mountKey] = &data.Metadata{
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
	metas := make([]*data.Metadata, 0, len(metaMap))
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

// CreateDirectory creates a new directory at the specified path.
// Returns an error if the directory already exists or cannot be created.
func (vfs *virtualFileSystemImpl) CreateDirectory(ctx context.Context, path string) error {
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
func (vfs *virtualFileSystemImpl) RemoveDirectory(ctx context.Context, path string, force bool) error {
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

	var stat *data.FileStat
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
func (vfs *virtualFileSystemImpl) UnlinkFile(ctx context.Context, path string) error {
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

	var stat *data.FileStat
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
// This implementation uses a copy-and-delete strategy which works across different mounts
// but is not atomic and may not be optimal for large files.
func (vfs *virtualFileSystemImpl) Rename(ctx context.Context, oldPath string, newPath string) error {
	// Convert to absolute paths
	oldAbsolute, err := data.ToAbsolutePath(oldPath)
	if err != nil {
		vfs.log.Error("Rename: failed to convert oldPath to absolute: %s - %v", oldPath, err)
		return err
	}

	newAbsolute, err := data.ToAbsolutePath(newPath)
	if err != nil {
		vfs.log.Error("Rename: failed to convert newPath to absolute: %s - %v", newPath, err)
		return err
	}

	vfs.log.Debug("Rename: renaming %s to %s", oldAbsolute, newAbsolute)

	// Prevent renaming mount points
	vfs.mu.RLock()
	_, isMountPoint := vfs.mnts[oldAbsolute]
	vfs.mu.RUnlock()

	if isMountPoint {
		vfs.log.Error("Rename: cannot rename mount point %s", oldAbsolute)
		return errors.PathMountBusy(nil, oldAbsolute)
	}

	// Get stat for old path to check existence and type
	oldStat, err := vfs.StatMetadata(ctx, oldAbsolute)
	if err != nil {
		vfs.log.Error("Rename: source path %s does not exist - %v", oldAbsolute, err)
		return err
	}

	// Check if newPath already exists
	if exists, _ := vfs.LookupMetadata(ctx, newAbsolute); exists {
		vfs.log.Error("Rename: destination path %s already exists", newAbsolute)
		return data.ErrExist
	}

	// Handle based on type
	if oldStat.Mode.IsDir() {
		vfs.log.Debug("Rename: renaming directory %s to %s", oldAbsolute, newAbsolute)
		return vfs.renameDirectory(ctx, oldAbsolute, newAbsolute)
	}

	// Handle file rename
	vfs.log.Debug("Rename: renaming file %s to %s (size=%d)", oldAbsolute, newAbsolute, oldStat.Size)
	return vfs.renameFile(ctx, oldAbsolute, newAbsolute, oldStat)
}
