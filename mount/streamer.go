package mount

import (
	"context"
	"io"
	"sync"

	"github.com/mwantia/vfs/data"
)

type mountStreamer struct {
	mu  sync.RWMutex
	ctx context.Context
	mnt MountPoint

	options *mountStreamerOptions
	closed  bool
}

type mountStreamerOptions struct {
	path       string
	offset     int64
	flags      data.AccessMode
	bufferSize int64
}

func newMountStreamer(ctx context.Context, mnt MountPoint, options *mountStreamerOptions) MountStreamer {
	if options.bufferSize <= 0 {
		// Default buffer size of 1 MB
		options.bufferSize = 1 * 1024 * 1024
	}

	return &mountStreamer{
		ctx: ctx,
		mnt: mnt,

		options: options,
		closed:  false,
	}
}

func (ms *mountStreamer) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the file is busy
	if !ms.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	ms.mu.Unlock()

	return false
}

// CanRead returns true if the virtual file can be read, otherwise false.
func (ms *mountStreamer) CanRead() bool {
	return ms.options.flags&data.AccessModeRead != 0
}

// CanWrite returns true if the virtual file can be written, otherwise false.
func (ms *mountStreamer) CanWrite() bool {
	return ms.options.flags&data.AccessModeWrite != 0
}

// Read reads up to len(p) bytes from the file at the current offset.
// Advances the offset by the number of bytes read.
// Returns ErrPermission if the file was not opened with read access.
func (ms *mountStreamer) Read(p []byte) (n int, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.closed {
		return 0, data.ErrClosed
	}
	// Check if file was opened with read access
	if !ms.CanRead() {
		return 0, data.ErrPermission
	}
	// Read from mount
	buffer, err := ms.mnt.ReadFile(ms.ctx, ms.options.path, ms.options.offset, int64(len(p)))
	if err != nil {
		return 0, err
	}
	// Copy to destination buffer
	n = copy(p, buffer)
	// Update offset
	ms.options.offset += int64(n)
	// Return EOF if no bytes read
	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

// Write writes len(p) bytes to the file at the current offset.
// Advances the offset by the number of bytes written.
// Returns ErrPermission if the file was not opened with write access.
func (ms *mountStreamer) Write(p []byte) (n int, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.closed {
		return 0, data.ErrClosed
	}
	// Check if file was opened with write access
	if !ms.CanWrite() {
		return 0, data.ErrPermission
	}
	// Write to mount
	n, err = ms.mnt.WriteFile(ms.ctx, ms.options.path, ms.options.offset, p)
	if err != nil {
		return 0, err
	}
	// Update offset
	ms.options.offset += int64(n)

	return n, nil
}

// Seek sets the offset for the next Read or Write operation and returns the new offset.
func (ms *mountStreamer) Seek(offset int64, whence int) (int64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.closed {
		return 0, data.ErrClosed
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = ms.options.offset + offset
	case io.SeekEnd:
		// Need to get file size to seek from end
		meta, err := ms.mnt.StatMetadata(ms.ctx, ms.options.path)
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

	ms.options.offset = newOffset
	return newOffset, nil
}

// Close marks the file stream as closed and unregisters it from the VFS.
func (ms *mountStreamer) Close() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.closed {
		return data.ErrClosed
	}
	// Close the file in the mount
	if err := ms.mnt.CloseFile(ms.ctx, ms.options.path, false); err != nil {
		return err
	}

	// Only mark as closed after successful close
	ms.closed = true
	return nil
}

func (ms *mountStreamer) ReadFrom(r io.Reader) (int64, error) {
	// Use a custom buffer for bulk reads
	buffer := make([]byte, ms.options.bufferSize)

	var total int64
	for {
		n, err := r.Read(buffer)
		if n > 0 {
			wrote, writeErr := ms.Write(buffer[:n])
			total += int64(wrote)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}

func (ms *mountStreamer) WriteTo(w io.Writer) (int64, error) {
	// Use a custom buffer for bulk writes
	buffer := make([]byte, ms.options.bufferSize)

	var total int64
	for {
		n, err := ms.Read(buffer)
		if n > 0 {
			wrote, writeErr := w.Write(buffer[:n])
			total += int64(wrote)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}
