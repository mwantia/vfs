package memory

import (
	"context"
	"time"

	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/data"
)

// GetInode returns the complete inode information for a path.
// Returns ErrNotExist if the path or inode doesn't exist.
//
// IMPORTANT: This method does NOT acquire locks. The caller must hold
// either a read lock (RLock) or write lock (Lock) before calling this method.
//
// Lookup flow: path → (B-tree) → inode ID → (map) → inode metadata
func (m *MetaMemoryMount) GetInode(ctx context.Context, path string) (*data.VirtualInode, error) {
	inodeID, exists := m.paths.Get(path)
	if !exists {
		return nil, vfs.ErrNotExist
	}

	inode, exists := m.inodes[inodeID]
	if !exists {
		return nil, vfs.ErrNotExist
	}

	// Update access time (acceptable race condition for performance)
	inode.AccessTime = time.Now()
	return inode, nil
}

// SetInode updates the inode information for a path.
// This allows changing timestamps, metadata, or even what data
// a path points to (for advanced operations).
// Returns ErrNotExist if the path doesn't exist.
// If nil is set for 'inode' it will be seen as a deletion attempt for this path.
//
// IMPORTANT: This method does NOT acquire locks. The caller must hold
// a write lock (Lock) before calling this method.
func (m *MetaMemoryMount) SetInode(ctx context.Context, path string, inode *data.VirtualInode, mask data.VirtualInodeUpdateMask) error {
	// Layer 1: Look up inode ID from path
	inodeID, exists := m.paths.Get(path)
	if !exists {
		return vfs.ErrNotExist
	}

	// Layer 2: Get target inode
	target, exists := m.inodes[inodeID]
	if !exists {
		return vfs.ErrNotExist
	}

	if inode == nil {
		// Delete this path mapping (for unlink operation)
		m.paths.Delete(path)

		// Check if any other paths reference this inode (hard links)
		hasOtherLinks := false
		m.paths.Scan(func(key string, value string) bool {
			if value == inodeID {
				hasOtherLinks = true
				return false // Stop scanning
			}
			return true // Continue scanning
		})

		// Only delete inode and data if no other links exist
		if !hasOtherLinks {
			delete(m.datas, target.ID)
			delete(m.inodes, inodeID)
		}

		return nil
	}

	update := data.VirtualInodeUpdate{
		Mask:  mask,
		Inode: inode,
	}

	n := time.Now()
	// Update AccessTime, ModifyTime and ChangeTime beforehand
	// These can still be overwritten via 'inode'
	target.AccessTime = n
	target.ModifyTime = n
	target.ChangeTime = n

	return update.Apply(target)
}
