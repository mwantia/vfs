package vfs

import (
	"context"
	"io"
	"sync"
)

// VirtualReader combines all read-related interfaces for VFS streaming.
// It provides read, seek, and close capabilities.
type VirtualReader interface {
	io.Reader
	io.Seeker
	io.Closer
}

type virtualReaderImpl struct {
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
func (vr *virtualReaderImpl) Read(p []byte) (n int, err error) {
	vr.mu.Lock()
	defer vr.mu.Unlock()

	if vr.closed {
		return 0, ErrClosed
	}

	// Check context cancellation
	select {
	case <-vr.ctx.Done():
		return 0, vr.ctx.Err()
	default:
	}

	// Read from mount at current offset
	n, err = vr.mount.Read(vr.ctx, vr.path, vr.offset, p)
	if n > 0 {
		vr.offset += int64(n)
	}

	return n, err
}

// Seek sets the offset for the next Read operation and returns the new offset.
// It implements io.Seeker.
func (vr *virtualReaderImpl) Seek(offset int64, whence int) (int64, error) {
	vr.mu.Lock()
	defer vr.mu.Unlock()

	if vr.closed {
		return 0, ErrClosed
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = vr.offset + offset
	case io.SeekEnd:
		// Need to get file size
		info, err := vr.mount.Stat(vr.ctx, vr.path)
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

	vr.offset = newOffset
	return newOffset, nil
}

// Close marks the reader as closed and unregisters it from the VFS.
func (vr *virtualReaderImpl) Close() error {
	vr.mu.Lock()
	defer vr.mu.Unlock()

	if vr.closed {
		return ErrClosed
	}

	vr.closed = true

	vr.vfs.mu.Lock()
	delete(vr.vfs.streams, vr.absPath)
	vr.vfs.mu.Unlock()

	return nil
}
