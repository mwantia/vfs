package mount

import (
	"io"
	"sync"

	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
)

type traversalMountStreamer struct {
	mu sync.RWMutex

	traversal context.TraversalContext
	mnt       MountPoint

	offset int64
	flags  data.AccessMode
	closed bool
}

func newTraversalMountStreamer(traversal context.TraversalContext, mnt MountPoint, offset int64, flags data.AccessMode) MountStreamer {
	return &traversalMountStreamer{
		traversal: traversal,
		mnt:       mnt,
		offset:    offset,
		flags:     flags,
		closed:    false,
	}
}

func (tms *traversalMountStreamer) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the file is busy
	if !tms.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	tms.mu.Unlock()

	return false
}

// CanRead returns true if the virtual file can be read, otherwise false.
func (tms *traversalMountStreamer) CanRead() bool {
	return tms.flags&data.AccessModeRead != 0
}

// CanWrite returns true if the virtual file can be written, otherwise false.
func (tms *traversalMountStreamer) CanWrite() bool {
	return tms.flags&data.AccessModeWrite != 0
}

// Read reads up to len(p) bytes from the file at the current offset.
// Advances the offset by the number of bytes read.
// Returns ErrPermission if the file was not opened with read access.
func (tms *traversalMountStreamer) Read(p []byte) (n int, err error) {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	// Fail operations if streamer has been closed
	if tms.closed {
		return 0, data.ErrClosed
	}
	// Check if file was opened with read access
	if !tms.CanRead() {
		return 0, data.ErrPermission
	}

	// Read from mount
	buffer, err := tms.mnt.ReadFile(tms.traversal, tms.offset, int64(len(p)))
	if err != nil {
		return 0, err
	}

	// Copy to destination buffer
	n = copy(p, buffer)

	// Update offset
	tms.offset += int64(n)

	// Return EOF if no bytes read
	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

// Write writes len(p) bytes to the file at the current offset.
// Advances the offset by the number of bytes written.
// Returns ErrPermission if the file was not opened with write access.
func (tms *traversalMountStreamer) Write(p []byte) (n int, err error) {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	// Fail operations if streamer has been closed
	if tms.closed {
		return 0, data.ErrClosed
	}
	// Check if file was opened with write access
	if !tms.CanWrite() {
		return 0, data.ErrPermission
	}

	// Write to mount
	n, err = tms.mnt.WriteFile(tms.traversal, tms.offset, p)
	if err != nil {
		return 0, err
	}

	// Update offset
	tms.offset += int64(n)

	return n, nil
}

// Seek sets the offset for the next Read or Write operation and returns the new offset.
func (tms *traversalMountStreamer) Seek(offset int64, whence int) (int64, error) {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	// Fail operations if streamer has been closed
	if tms.closed {
		return 0, data.ErrClosed
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = tms.offset + offset
	case io.SeekEnd:
		// Need to get file size to seek from end
		meta, err := tms.mnt.StatMetadata(tms.traversal)
		if err != nil {
			return 0, err
		}
		newOffset = meta.Size + offset
	default:
		return 0, data.ErrInvalid
	}

	// Ensure offset is non-negative
	if newOffset < 0 {
		return 0, data.ErrInvalid
	}

	tms.offset = newOffset
	return newOffset, nil
}

// Close marks the file stream as closed and unregisters it from the VFS.
func (tms *traversalMountStreamer) Close() error {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	// Fail operations if streamer has been closed
	if tms.closed {
		return data.ErrClosed
	}

	// Close the file in the mount
	if err := tms.mnt.CloseFile(tms.traversal, false); err != nil {
		return err
	}

	// Only mark as closed after successful close
	tms.closed = true
	return nil
}
