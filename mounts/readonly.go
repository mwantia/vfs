package mounts

import (
	"context"

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

// GetCapabilities returns the capabilities of the underlying mount.
// Note: The capabilities don't reflect the read-only restriction.
func (rom *ReadOnlyMount) GetCapabilities() vfs.VirtualMountCapabilities {
	return rom.mount.GetCapabilities()
}

// Stat returns information about an object by delegating to the underlying mount.
func (rom *ReadOnlyMount) Stat(ctx context.Context, path string) (*vfs.VirtualObjectInfo, error) {
	return rom.mount.Stat(ctx, path)
}

// List returns directory contents by delegating to the underlying mount.
func (rom *ReadOnlyMount) List(ctx context.Context, path string) ([]*vfs.VirtualObjectInfo, error) {
	return rom.mount.List(ctx, path)
}

// Read reads data from the file by delegating to the underlying mount.
func (rom *ReadOnlyMount) Read(ctx context.Context, path string, offset int64, data []byte) (int, error) {
	return rom.mount.Read(ctx, path, offset, data)
}

// Write always returns ErrReadOnly as this mount is read-only.
func (rom *ReadOnlyMount) Write(ctx context.Context, path string, offset int64, data []byte) (int, error) {
	return 0, vfs.ErrReadOnly
}

// Create always returns ErrReadOnly as this mount is read-only.
func (rom *ReadOnlyMount) Create(ctx context.Context, path string, isDir bool) error {
	return vfs.ErrReadOnly
}

// Delete always returns ErrReadOnly as this mount is read-only.
func (rom *ReadOnlyMount) Delete(ctx context.Context, path string, force bool) error {
	return vfs.ErrReadOnly
}

// Truncate always returns ErrReadOnly as this mount is read-only.
func (rom *ReadOnlyMount) Truncate(ctx context.Context, path string, size int64) error {
	return vfs.ErrReadOnly
}
