package mounts

import (
	"context"
	"io"

	"github.com/mwantia/vfs"
)

// ReadOnly wraps any Mount implementation to make it read-only.
// All read operations are passed through to the underlying mount.
// All write operations return ErrReadOnly.
type ReadOnlyMount struct {
	mount vfs.VirtualMount
}

// NewReadOnly creates a new read-only wrapper around the given mount.
func NewReadOnly(mount vfs.VirtualMount) *ReadOnlyMount {
	return &ReadOnlyMount{
		mount: mount,
	}
}

// Stat passes through to the underlying mount.
func (rom *ReadOnlyMount) Stat(ctx context.Context, path string) (*vfs.VirtualFileInfo, error) {
	return rom.mount.Stat(ctx, path)
}

// ReadDir passes through to the underlying mount.
func (rom *ReadOnlyMount) ReadDir(ctx context.Context, path string) ([]*vfs.VirtualFileInfo, error) {
	return rom.mount.ReadDir(ctx, path)
}

// Open passes through to the underlying mount.
func (rom *ReadOnlyMount) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	return rom.mount.Open(ctx, path)
}

// Create returns ErrReadOnly.
func (*ReadOnlyMount) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return nil, vfs.ErrReadOnly
}

// Remove returns ErrReadOnly.
func (*ReadOnlyMount) Remove(ctx context.Context, path string) error {
	return vfs.ErrReadOnly
}

// Mkdir returns ErrReadOnly.
func (*ReadOnlyMount) Mkdir(ctx context.Context, path string) error {
	return vfs.ErrReadOnly
}

// RemoveAll returns ErrReadOnly.
func (*ReadOnlyMount) RemoveAll(ctx context.Context, path string) error {
	return vfs.ErrReadOnly
}
