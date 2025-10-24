package vfs

import (
	"context"
	"io"
	"sync"

	"github.com/mwantia/vfs/data"
)

// VirtualStream combines all file operation interfaces for VFS streaming.
// It provides read, write, seek, and close capabilities.
// The available operations depend on the access mode flags used when opening.
type VirtualStream interface {
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

type VirtualFileStream struct {
	mu  sync.RWMutex
	ctx context.Context

	vfs    *VirtualFileSystem
	mnt    *VirtualMountEntry
	path   string
	offset int64
	flags  data.VirtualAccessMode
	closed bool
}

func NewVirtualFileStream(ctx context.Context, vfs *VirtualFileSystem, mnt *VirtualMountEntry, path string, offset int64, flags data.VirtualAccessMode) *VirtualFileStream {
	return &VirtualFileStream{
		ctx: ctx,

		vfs:    vfs,
		mnt:    mnt,
		path:   path,
		offset: offset,
		flags:  flags,
	}
}

// Read reads up to len(p) bytes from the file at the current offset.
// Advances the offset by the number of bytes read.
// Returns ErrPermission if the file was not opened with read access.
func (vfs *VirtualFileStream) Read(p []byte) (n int, err error) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if vfs.closed {
		return 0, data.ErrClosed
	}

	if !vfs.flags.IsReadOnly() && !vfs.flags.IsReadWrite() {
		return 0, data.ErrPermission
	}

	// Check context cancellation
	select {
	case <-vfs.ctx.Done():
		return 0, vfs.ctx.Err()
	default:
	}

	// Read from mount at current offset
	n, err = vfs.mnt.mount.Read(vfs.ctx, vfs.path, vfs.offset, p)
	if n > 0 {
		vfs.offset += int64(n)
	}

	return n, err
}

// Write writes len(p) bytes to the file at the current offset.
// Advances the offset by the number of bytes written.
// Returns ErrPermission if the file was not opened with write access.
func (vfs *VirtualFileStream) Write(p []byte) (n int, err error) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if vfs.closed {
		return 0, data.ErrClosed
	}

	if !vfs.flags.IsWriteOnly() && !vfs.flags.IsReadWrite() {
		return 0, data.ErrPermission
	}

	// Check context cancellation
	select {
	case <-vfs.ctx.Done():
		return 0, vfs.ctx.Err()
	default:
	}

	// Write to mount at current offset
	n, err = vfs.mnt.mount.Write(vfs.ctx, vfs.path, vfs.offset, p)
	if n > 0 {
		vfs.offset += int64(n)
	}

	return n, err
}

// Seek sets the offset for the next Read or Write operation and returns the new offset.
// It implements io.Seeker.
func (vfs *VirtualFileStream) Seek(offset int64, whence int) (int64, error) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if vfs.closed {
		return 0, data.ErrClosed
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = vfs.offset + offset
	case io.SeekEnd:
		// Need to get file size
		info, err := vfs.mnt.mount.Stat(vfs.ctx, vfs.path)
		if err != nil {
			return 0, err
		}
		newOffset = info.Size + offset
	default:
		return 0, data.ErrInvalid
	}

	if newOffset < 0 {
		return 0, data.ErrInvalid
	}

	vfs.offset = newOffset
	return newOffset, nil
}

// Close marks the file stream as closed and unregisters it from the VFS.
func (vfs *VirtualFileStream) Close() error {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if vfs.closed {
		return data.ErrClosed
	}

	vfs.closed = true

	vfs.vfs.mu.Lock()
	delete(vfs.vfs.streams, vfs.mnt.path+vfs.path)
	vfs.vfs.mu.Unlock()

	return nil
}

func (vfs *VirtualFileStream) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the file is busy
	if !vfs.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	vfs.mu.Unlock()
	return false
}

// CanRead returns true if the virtual file can be read, otherwise false.
func (vfs *VirtualFileStream) CanRead() bool {
	return vfs.flags&data.AccessModeRead != 0
}

// CanWrite returns true if the virtual file can be written, otherwise false.
func (vfs *VirtualFileStream) CanWrite() bool {
	return vfs.flags&data.AccessModeWrite != 0
}
