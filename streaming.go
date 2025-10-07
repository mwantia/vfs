package vfs

import (
	"context"
	"sync"
)

// VirtualReadCloser provides streaming read access to a file via offset-based mount operations.
type VirtualReadCloser struct {
	mount  VirtualMount
	path   string
	offset int64

	mu     sync.RWMutex
	ctx    context.Context
	closed bool
}

// NewVirtualReadCloser creates a new streaming reader for the given file.
func NewVirtualReadCloser(ctx context.Context, mount VirtualMount, path string) *VirtualReadCloser {
	return &VirtualReadCloser{
		mount:  mount,
		path:   path,
		offset: 0,
		ctx:    ctx,
		closed: false,
	}
}

// Read reads up to len(p) bytes from the file at the current offset.
// Advances the offset by the number of bytes read.
func (vrc *VirtualReadCloser) Read(p []byte) (n int, err error) {
	vrc.mu.Lock()
	defer vrc.mu.Unlock()

	if vrc.closed {
		return 0, ErrClosed
	}

	// Check context cancellation
	select {
	case <-vrc.ctx.Done():
		return 0, vrc.ctx.Err()
	default:
	}

	// Read from mount at current offset
	n, err = vrc.mount.Read(vrc.ctx, vrc.path, vrc.offset, p)
	if n > 0 {
		vrc.offset += int64(n)
	}

	return n, err
}

// Close marks the reader as closed. No-op for read-only streams.
func (vrc *VirtualReadCloser) Close() error {
	vrc.mu.Lock()
	defer vrc.mu.Unlock()

	if vrc.closed {
		return ErrClosed
	}

	vrc.closed = true
	return nil
}

// VirtualReadWriteCloser provides streaming read/write access to a file via offset-based mount operations.
// Writes are applied immediately to the mount at the current offset.
type VirtualReadWriteCloser struct {
	mount       VirtualMount
	path        string
	readOffset  int64
	writeOffset int64

	mu     sync.RWMutex
	ctx    context.Context
	closed bool
}

// NewVirtualReadWriteCloser creates a new streaming reader/writer for the given file.
// If the file doesn't exist, it should be created first using mount.Create().
func NewVirtualReadWriteCloser(ctx context.Context, mount VirtualMount, path string) *VirtualReadWriteCloser {
	return &VirtualReadWriteCloser{
		mount:       mount,
		path:        path,
		readOffset:  0,
		writeOffset: 0,
		ctx:         ctx,
		closed:      false,
	}
}

// Read reads up to len(p) bytes from the file at the current read offset.
// Advances the read offset by the number of bytes read.
func (vwc *VirtualReadWriteCloser) Read(p []byte) (n int, err error) {
	vwc.mu.Lock()
	defer vwc.mu.Unlock()

	if vwc.closed {
		return 0, ErrClosed
	}

	// Check context cancellation
	select {
	case <-vwc.ctx.Done():
		return 0, vwc.ctx.Err()
	default:
	}

	// Read from mount at current read offset
	n, err = vwc.mount.Read(vwc.ctx, vwc.path, vwc.readOffset, p)
	if n > 0 {
		vwc.readOffset += int64(n)
	}

	return n, err
}

// Write writes len(p) bytes to the file at the current write offset.
// Advances the write offset by the number of bytes written.
// Data is written immediately to the underlying mount.
func (vwc *VirtualReadWriteCloser) Write(p []byte) (n int, err error) {
	vwc.mu.Lock()
	defer vwc.mu.Unlock()

	if vwc.closed {
		return 0, ErrClosed
	}

	// Check context cancellation
	select {
	case <-vwc.ctx.Done():
		return 0, vwc.ctx.Err()
	default:
	}

	// Write to mount at current write offset
	n, err = vwc.mount.Write(vwc.ctx, vwc.path, vwc.writeOffset, p)
	if n > 0 {
		vwc.writeOffset += int64(n)
	}

	return n, err
}

// Close marks the stream as closed. No flush needed as writes are immediate.
func (vwc *VirtualReadWriteCloser) Close() error {
	vwc.mu.Lock()
	defer vwc.mu.Unlock()

	if vwc.closed {
		return ErrClosed
	}

	vwc.closed = true
	return nil
}
