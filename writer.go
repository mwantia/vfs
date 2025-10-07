package vfs

import (
	"context"
	"io"
	"sync"
)

// VirtualReadWriter combines all read/write-related interfaces for VFS streaming.
// It provides read, write, seek, and close capabilities.
type VirtualReadWriter interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

type virtualWriterImpl struct {
	vfs     *VirtualFileSystem
	mount   VirtualMount
	path    string // Relative path for mount operations
	absPath string // Absolute path for stream tracking
	offset  int64
	closed  bool

	mu  sync.RWMutex
	ctx context.Context
}

// Read reads up to len(p) bytes from the file at the current offset.
// Advances the offset by the number of bytes read.
func (vw *virtualWriterImpl) Read(p []byte) (n int, err error) {
	vw.mu.Lock()
	defer vw.mu.Unlock()

	if vw.closed {
		return 0, ErrClosed
	}

	// Check context cancellation
	select {
	case <-vw.ctx.Done():
		return 0, vw.ctx.Err()
	default:
	}

	// Read from mount at current offset
	n, err = vw.mount.Read(vw.ctx, vw.path, vw.offset, p)
	if n > 0 {
		vw.offset += int64(n)
	}

	return n, err
}

// Write writes len(p) bytes to the file at the current offset.
// Advances the offset by the number of bytes written.
// Data is written immediately to the underlying mount.
func (vw *virtualWriterImpl) Write(p []byte) (n int, err error) {
	vw.mu.Lock()
	defer vw.mu.Unlock()

	if vw.closed {
		return 0, ErrClosed
	}

	// Check context cancellation
	select {
	case <-vw.ctx.Done():
		return 0, vw.ctx.Err()
	default:
	}

	// Write to mount at current offset
	n, err = vw.mount.Write(vw.ctx, vw.path, vw.offset, p)
	if n > 0 {
		vw.offset += int64(n)
	}

	return n, err
}

// Seek sets the offset for the next Read or Write operation and returns the new offset.
// It implements io.Seeker.
func (vw *virtualWriterImpl) Seek(offset int64, whence int) (int64, error) {
	vw.mu.Lock()
	defer vw.mu.Unlock()

	if vw.closed {
		return 0, ErrClosed
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = vw.offset + offset
	case io.SeekEnd:
		// Need to get file size
		info, err := vw.mount.Stat(vw.ctx, vw.path)
		if err != nil {
			return 0, err
		}
		newOffset = info.Size + offset
	default:
		return 0, ErrInvalid
	}

	if newOffset < 0 {
		return 0, ErrInvalid
	}

	vw.offset = newOffset
	return newOffset, nil
}

// Close marks the stream as closed and unregisters it from the VFS.
func (vw *virtualWriterImpl) Close() error {
	vw.mu.Lock()
	defer vw.mu.Unlock()

	if vw.closed {
		return ErrClosed
	}

	vw.closed = true

	vw.vfs.mu.Lock()
	delete(vw.vfs.streams, vw.absPath)
	vw.vfs.mu.Unlock()

	return nil
}
