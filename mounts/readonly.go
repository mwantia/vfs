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

func (rom *ReadOnlyMount) GetCapabilities() vfs.VirtualMountCapabilities {
	return rom.mount.GetCapabilities()
}

func (rom *ReadOnlyMount) Stat(ctx context.Context, path string) (*vfs.VirtualObjectInfo, error) {
	return rom.mount.Stat(ctx, path)
}

func (rom *ReadOnlyMount) List(ctx context.Context, path string) ([]*vfs.VirtualObjectInfo, error) {
	return rom.mount.List(ctx, path)
}

func (rom *ReadOnlyMount) Get(ctx context.Context, path string) (*vfs.VirtualObject, error) {
	return rom.mount.Get(ctx, path)
}

func (rom *ReadOnlyMount) Create(ctx context.Context, path string, obj *vfs.VirtualObject) error {
	return vfs.ErrReadOnly
}

func (rom *ReadOnlyMount) Update(ctx context.Context, path string, obj *vfs.VirtualObject) (bool, error) {
	return false, vfs.ErrReadOnly
}

func (rom *ReadOnlyMount) Delete(ctx context.Context, path string, force bool) (bool, error) {
	return false, vfs.ErrReadOnly
}

func (rom *ReadOnlyMount) Upsert(ctx context.Context, path string, source any) error {
	return vfs.ErrReadOnly
}
