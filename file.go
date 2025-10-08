package vfs

import (
	"context"
	"io"
	"sync"
)

// VirtualFile combines all file operation interfaces for VFS streaming.
// It provides read, write, seek, and close capabilities.
// The available operations depend on the access mode flags used when opening.
type VirtualFile interface {
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

// virtualFileImpl is the unified implementation for file streams.
// It supports both read and write operations based on access mode flags.
type virtualFileImpl struct {
	vfs     *VirtualFileSystem
	mount   VirtualMount
	path    string // Relative path for mount operations
	absPath string // Absolute path for stream tracking
	offset  int64
	flags   VirtualAccessMode
	closed  bool

	mu  sync.RWMutex
	ctx context.Context
}

// Read reads up to len(p) bytes from the file at the current offset.
// Advances the offset by the number of bytes read.
// Returns ErrPermission if the file was not opened with read access.
func (vf *virtualFileImpl) Read(p []byte) (n int, err error) {
	vf.mu.Lock()
	defer vf.mu.Unlock()

	if vf.closed {
		return 0, ErrClosed
	}

	if !vf.flags.IsReadOnly() && !vf.flags.IsReadWrite() {
		return 0, ErrPermission
	}

	// Check context cancellation
	select {
	case <-vf.ctx.Done():
		return 0, vf.ctx.Err()
	default:
	}

	// Read from mount at current offset
	n, err = vf.mount.Read(vf.ctx, vf.path, vf.offset, p)
	if n > 0 {
		vf.offset += int64(n)
	}

	return n, err
}

// Write writes len(p) bytes to the file at the current offset.
// Advances the offset by the number of bytes written.
// Returns ErrPermission if the file was not opened with write access.
func (vf *virtualFileImpl) Write(p []byte) (n int, err error) {
	vf.mu.Lock()
	defer vf.mu.Unlock()

	if vf.closed {
		return 0, ErrClosed
	}

	if !vf.flags.IsWriteOnly() && !vf.flags.IsReadWrite() {
		return 0, ErrPermission
	}

	// Check context cancellation
	select {
	case <-vf.ctx.Done():
		return 0, vf.ctx.Err()
	default:
	}

	// Write to mount at current offset
	n, err = vf.mount.Write(vf.ctx, vf.path, vf.offset, p)
	if n > 0 {
		vf.offset += int64(n)
	}

	return n, err
}

// Seek sets the offset for the next Read or Write operation and returns the new offset.
// It implements io.Seeker.
func (vf *virtualFileImpl) Seek(offset int64, whence int) (int64, error) {
	vf.mu.Lock()
	defer vf.mu.Unlock()

	if vf.closed {
		return 0, ErrClosed
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = vf.offset + offset
	case io.SeekEnd:
		// Need to get file size
		info, err := vf.mount.Stat(vf.ctx, vf.path)
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

	vf.offset = newOffset
	return newOffset, nil
}

// Close marks the file stream as closed and unregisters it from the VFS.
func (vf *virtualFileImpl) Close() error {
	vf.mu.Lock()
	defer vf.mu.Unlock()

	if vf.closed {
		return ErrClosed
	}

	vf.closed = true

	vf.vfs.mu.Lock()
	delete(vf.vfs.streams, vf.absPath)
	vf.vfs.mu.Unlock()

	return nil
}

func (vf *virtualFileImpl) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the file is busy
	if !vf.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	vf.mu.Unlock()
	return false
}

// CanRead returns true if the virtual file can be read, otherwise false.
func (vf *virtualFileImpl) CanRead() bool {
	return vf.flags&AccessModeRead != 0
}

// CanWrite returns true if the virtual file can be written, otherwise false.
func (vf *virtualFileImpl) CanWrite() bool {
	return vf.flags&AccessModeWrite != 0
}
