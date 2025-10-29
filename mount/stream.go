package mount

import (
	"context"
	"io"
	"sync"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
	"github.com/mwantia/vfs/log"
)

// Streamer combines all operation interfaces for data-streaming.
// It provides read, write, seek, and close capabilities.
// The available operations depend on the access mode flags used when opening.
type Streamer interface {
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

type MountStreamer struct {
	mu  sync.RWMutex
	ctx context.Context
	log *log.Logger

	mnt    *Mount
	path   string
	offset int64
	flags  data.VirtualAccessMode
	closed bool
}

func newMountStreamer(ctx context.Context, log *log.Logger, mnt *Mount, path string, offset int64, flags data.VirtualAccessMode) *MountStreamer {
	return &MountStreamer{
		ctx:    ctx,
		log:    log,
		mnt:    mnt,
		path:   path,
		offset: offset,
		flags:  flags,
	}
}

func (ms *MountStreamer) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the file is busy
	if !ms.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	ms.mu.Unlock()

	return false
}

// CanRead returns true if the virtual file can be read, otherwise false.
func (ms *MountStreamer) CanRead() bool {
	return ms.flags&data.AccessModeRead != 0
}

// CanWrite returns true if the virtual file can be written, otherwise false.
func (ms *MountStreamer) CanWrite() bool {
	return ms.flags&data.AccessModeWrite != 0
}

// Read reads up to len(p) bytes from the file at the current offset.
// Advances the offset by the number of bytes read.
// Returns ErrPermission if the file was not opened with read access.
func (ms *MountStreamer) Read(p []byte) (n int, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.closed {
		ms.log.Error("Read: attempted to read from closed streamer for %s", ms.path)
		return 0, data.ErrClosed
	}

	ms.log.Debug("Read: reading up to %d bytes from %s at offset %d", len(p), ms.path, ms.offset)

	if !ms.flags.IsReadOnly() && !ms.flags.IsReadWrite() {
		ms.log.Error("Read: no read permission for %s (flags=%v)", ms.path, ms.flags)
		return 0, data.ErrPermission
	}

	select {
	case <-ms.ctx.Done():
		ms.log.Error("Read: context cancelled for %s", ms.path)
		return 0, ms.ctx.Err()
	default:
	}

	n, err = ms.mnt.ObjectStorage.ReadObject(ms.ctx, ms.path, ms.offset, p)
	if n > 0 {
		ms.offset += int64(n)
		ms.log.Debug("Read: read %d bytes from %s, new offset=%d", n, ms.path, ms.offset)
	}

	if err != nil && err != io.EOF {
		ms.log.Error("Read: failed to read from %s - %v", ms.path, err)
	}

	return n, err
}

// Write writes len(p) bytes to the file at the current offset.
// Advances the offset by the number of bytes written.
// Returns ErrPermission if the file was not opened with write access.
func (ms *MountStreamer) Write(p []byte) (n int, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.closed {
		ms.log.Error("Write: attempted to write to closed streamer for %s", ms.path)
		return 0, data.ErrClosed
	}

	ms.log.Debug("Write: writing %d bytes to %s at offset %d", len(p), ms.path, ms.offset)

	if !ms.flags.IsWriteOnly() && !ms.flags.IsReadWrite() {
		ms.log.Error("Write: no write permission for %s (flags=%v)", ms.path, ms.flags)
		return 0, data.ErrPermission
	}

	// Check context cancellation
	select {
	case <-ms.ctx.Done():
		ms.log.Error("Write: context cancelled for %s", ms.path)
		return 0, ms.ctx.Err()
	default:
	}
	// Get current file size for validation
	var currentSize int64
	// Validate using metadata first if available
	if ms.mnt.Metadata != nil {
		ms.log.Debug("Write: validating file using metadata for %s", ms.path)
		meta, err := ms.mnt.Metadata.ReadMeta(ms.ctx, ms.path)
		if err != nil {
			ms.log.Debug("Write: metadata not found, falling back to object storage for %s", ms.path)
			// Metadata doesn't exist - try to populate from storage
			stat, statErr := ms.mnt.ObjectStorage.HeadObject(ms.ctx, ms.path)
			if statErr != nil {
				ms.log.Error("Write: failed to read object stat for %s - %v", ms.path, statErr)
				return 0, statErr
			}
			// Create metadata from storage (only if separate backend)
			meta = stat.ToMetadata()
			if !ms.mnt.IsDualMount && ms.mnt.Metadata != nil {
				ms.log.Debug("Write: syncing object stat to metadata for %s", ms.path)
				if createErr := ms.mnt.Metadata.CreateMeta(ms.ctx, meta); createErr != nil {
					ms.log.Error("Write: failed to sync metadata for %s - %v", ms.path, createErr)
					return 0, createErr
				}
			}
		}

		// Validate it's not a directory
		if meta.Mode.IsDir() {
			ms.log.Error("Write: cannot write to directory %s", ms.path)
			return 0, data.ErrIsDirectory
		}
		currentSize = meta.Size
	} else {
		// No metadata backend - get size from object storage
		ms.log.Debug("Write: getting file size from object storage for %s", ms.path)
		stat, statErr := ms.mnt.ObjectStorage.HeadObject(ms.ctx, ms.path)
		if statErr != nil {
			ms.log.Error("Write: failed to read object stat for %s - %v", ms.path, statErr)
			return 0, statErr
		}
		// Validate it's not a directory
		if stat.Mode.IsDir() {
			ms.log.Error("Write: cannot write to directory %s", ms.path)
			return 0, data.ErrIsDirectory
		}
		currentSize = stat.Size
	}

	// Calculate the final size after this write
	newSize := ms.offset + int64(len(p))
	if currentSize > newSize {
		// If writing in the middle of a file, the size doesn't change
		newSize = currentSize
	}

	// Validate the size against backend capabilities
	caps := ms.mnt.ObjectStorage.GetCapabilities()
	ms.log.Debug("Write: validating size (current=%d new=%d) for %s", currentSize, newSize, ms.path)

	// Check minimum size if set (0 means no minimum)
	if caps.MinObjectSize > 0 && newSize < caps.MinObjectSize {
		ms.log.Error("Write: object size %d bytes is below minimum %d bytes for %s", newSize, caps.MinObjectSize, ms.path)
		return 0, errors.BackendObjectTooSmall(nil, newSize, caps.MinObjectSize)
	}

	// Check maximum size if set (0 means no maximum)
	if caps.MaxObjectSize > 0 && newSize > caps.MaxObjectSize {
		ms.log.Error("Write: object size %d bytes exceeds maximum %d bytes for %s", newSize, caps.MaxObjectSize, ms.path)
		return 0, errors.BackendObjectTooLarge(nil, newSize, caps.MaxObjectSize)
	}

	// Write to storage backend
	ms.log.Debug("Write: writing to object storage for %s", ms.path)
	n, err = ms.mnt.ObjectStorage.WriteObject(ms.ctx, ms.path, ms.offset, p)
	if err != nil {
		ms.log.Error("Write: failed to write to object storage for %s - %v", ms.path, err)
		return n, err
	}

	if n > 0 {
		ms.offset += int64(n)
		ms.log.Debug("Write: wrote %d bytes to %s, new offset=%d", n, ms.path, ms.offset)
		// Update metadata if available
		if ms.mnt.Metadata != nil {
			ms.log.Debug("Write: updating metadata size for %s (new_size=%d)", ms.path, ms.offset)
			update := &data.VirtualFileMetadataUpdate{
				Mask: data.VirtualFileMetadataUpdateSize,
				Metadata: &data.VirtualFileMetadata{
					Size: ms.offset,
				},
			}

			if err := ms.mnt.Metadata.UpdateMeta(ms.ctx, ms.path, update); err != nil {
				ms.log.Warn("Write: failed to update metadata for %s - %v", ms.path, err)
				return 0, err
			}
		}
	}

	return n, err
}

// Seek sets the offset for the next Read or Write operation and returns the new offset.
func (ms *MountStreamer) Seek(offset int64, whence int) (int64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.closed {
		ms.log.Error("Seek: attempted to seek on closed streamer for %s", ms.path)
		return 0, data.ErrClosed
	}

	ms.log.Debug("Seek: seeking %s (offset=%d whence=%d)", ms.path, offset, whence)

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
		ms.log.Debug("Seek: SeekStart - setting offset to %d for %s", newOffset, ms.path)
	case io.SeekCurrent:
		newOffset = ms.offset + offset
		ms.log.Debug("Seek: SeekCurrent - setting offset to %d for %s", newOffset, ms.path)
	case io.SeekEnd:
		ms.log.Debug("Seek: SeekEnd - need to determine file size for %s", ms.path)
		// Should be avoided at all cost, since we need to get the file size
		if ms.mnt.Metadata != nil {
			ms.log.Debug("Seek: getting file size from metadata for %s", ms.path)
			meta, err := ms.mnt.Metadata.ReadMeta(ms.ctx, ms.path)
			if err != nil {
				ms.log.Error("Seek: failed to read metadata for %s - %v", ms.path, err)
				return 0, err
			}
			newOffset = meta.Size + offset
			ms.log.Debug("Seek: file size from metadata is %d, new offset=%d for %s", meta.Size, newOffset, ms.path)
		} else {
			ms.log.Debug("Seek: getting file size from object storage for %s", ms.path)
			// Fallback to storage to create metadata from object
			stat, err := ms.mnt.ObjectStorage.HeadObject(ms.ctx, ms.path)
			if err != nil {
				ms.log.Error("Seek: failed to read object stat for %s - %v", ms.path, err)
				return 0, err
			}
			newOffset = stat.Size + offset
			ms.log.Debug("Seek: file size from storage is %d, new offset=%d for %s", stat.Size, newOffset, ms.path)
			// Sync to metadata if available AND it's a separate backend instance
			if ms.mnt.Metadata != nil && !ms.mnt.IsDualMount {
				ms.log.Debug("Seek: syncing object stat to metadata for %s", ms.path)
				meta := stat.ToMetadata()
				if err := ms.mnt.Metadata.CreateMeta(ms.ctx, meta); err != nil {
					ms.log.Warn("Seek: failed to sync metadata for %s - %v", ms.path, err)
					return 0, err
				}
			}
		}
	default:
		ms.log.Error("Seek: invalid whence value %d for %s", whence, ms.path)
		return 0, data.ErrInvalid
	}

	if newOffset < 0 {
		ms.log.Error("Seek: invalid negative offset %d for %s", newOffset, ms.path)
		return 0, data.ErrInvalid
	}

	ms.offset = newOffset
	ms.log.Debug("Seek: successfully set offset to %d for %s", newOffset, ms.path)
	return newOffset, nil
}

// Close marks the file stream as closed and unregisters it from the VFS.
func (ms *MountStreamer) Close() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.closed {
		ms.log.Error("Close: attempted to close already closed streamer for %s", ms.path)
		return data.ErrClosed
	}

	ms.log.Debug("Close: closing streamer for %s", ms.path)

	ms.mnt.mu.Lock()
	defer ms.mnt.mu.Unlock()

	ms.closed = true
	delete(ms.mnt.streamers, ms.path)

	ms.log.Debug("Close: streamer closed for %s", ms.path)
	return nil
}
