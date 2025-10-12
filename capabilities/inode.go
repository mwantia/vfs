package capabilities

import (
	"context"

	"github.com/mwantia/vfs/data"
)

// VirtualMountInodeCapability extends VirtualMount with inode-level operations.
// Mounts that implement this interface can provide optimized operations
// for rename, hard links, and metadata management.
type VirtualMountInodeCapability interface {
	// GetInode returns the complete inode information for a path.
	// Returns ErrNotExist if the information doesn't exist.
	GetInode(ctx context.Context, path string) (*data.VirtualInode, error)

	// SetInode updates the inode information for a path.
	// This allows changing timestamps, metadata, or even what data
	// a path points to (for advanced operations).
	// Returns ErrNotExist if the path doesn't exist.
	// If nil is set for 'inode' it will be seen as a deletion attempt for this path.
	SetInode(ctx context.Context, path string, inode *data.VirtualInode, mask data.VirtualInodeUpdateMask) error
}
