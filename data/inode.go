package data

import (
	"encoding/json"
	"maps"
	"time"
)

// VirtualInode represents the complete inode information for a filesystem object.
// This separates the object's identity and content from its location in the namespace.
// Mounts that support inodes can provide significant optimizations for operations
// like rename, hard links, and metadata updates.
type VirtualInode struct {
	// Identity - unique identifier
	ID string `json:"id"`

	// Base name of the inode
	Name string `json:"name"`

	// Type of object (file, directory, etc.)
	Type VirtualFileType `json:"type"`

	// Unix-style mode and permissions
	Mode VirtualFileMode `json:"mode"`

	// Size in bytes (0 for directories)
	Size int64 `json:"size"`

	// Defines an inode as "deleted"
	MarkedAsDeleted bool `json:"marked_as_deleted,omitempty"`

	AccessTime time.Time `json:"access_time"`
	ModifyTime time.Time `json:"modify_time"`
	ChangeTime time.Time `json:"change_time"`
	CreateTime time.Time `json:"create_time"`

	// User ownership identity
	UID int64 `json:"uid,omitempty"`

	// Group ownership identity
	GID int64 `json:"gid,omitempty"`

	// Extended metadata (mount-specific)
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ToFileInfo converts an inode to basic object info.
// This allows inode-aware mounts to easily provide VirtualObjectInfo.
func (vi *VirtualInode) ToFileInfo(path string) *VirtualFileInfo {
	return &VirtualFileInfo{
		Path:       path,
		Type:       vi.Type,
		Size:       vi.Size,
		Mode:       vi.Mode,
		ModifyTime: vi.ModifyTime,
		Inode:      vi.ID,
	}
}

// Marshal provides JSON serialization for VirtualInode.
func (vi *VirtualInode) Marshal() ([]byte, error) {
	return json.Marshal(vi)
}

// Unmarshal provides JSON deserialization for VirtualInode.
func (vi *VirtualInode) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &vi)
}

// IsDir returns true if this object is a directory.
func (vi *VirtualInode) IsDir() bool {
	return vi.Type == NodeTypeDirectory || vi.Type == NodeTypeMount
}

// IsFile returns true if this object is a regular file.
func (vi *VirtualInode) IsFile() bool {
	return vi.Type == NodeTypeFile
}

// IsSymlink returns true if this object is a symbolic link.
func (vi *VirtualInode) IsSymlink() bool {
	return vi.Type == NodeTypeSymlink
}

// Clone creates a deep copy of the object info.
func (vi *VirtualInode) Clone() *VirtualInode {
	clone := *vi
	return &clone
}

// CloneMetadata creates a copy of metadata.
func (vi *VirtualInode) CloneMetadata() map[string]string {
	if vi.Metadata == nil {
		return nil
	}

	clone := make(map[string]string, len(vi.Metadata))
	maps.Copy(clone, vi.Metadata)

	return clone
}

// VirtualInodeUpdateMask controls which fields of an inode should be updated.
// This allows partial updates without needing to fetch and write back entire inodes.
type VirtualInodeUpdateMask int

const (
	InodeUpdateMode       VirtualInodeUpdateMask = 1 << iota // Update Mode (permissions)
	InodeUpdateSize                                          // Update Size
	InodeUpdateAccessTime                                    // Update AccessTime
	InodeUpdateModifyTime                                    // Update ModifyTime
	InodeUpdateChangeTime                                    // Update ChangeTime
	InodeUpdateUID                                           // Update UID
	InodeUpdateGID                                           // Update GID
	InodeUpdateMetadata                                      // Update Metadata map

	InodeUpdateAll = ^VirtualInodeUpdateMask(0) // Update all fields
)

// VirtualInodeUpdate represents a partial update to an inode.
type VirtualInodeUpdate struct {
	Mask  VirtualInodeUpdateMask `json:"mask"`
	Inode *VirtualInode          `json:"inode"`
}

// Apply applies this update to an existing inode.
func (u *VirtualInodeUpdate) Apply(target *VirtualInode) error {
	if u.Mask&InodeUpdateMode != 0 {
		target.Mode = u.Inode.Mode
	}
	if u.Mask&InodeUpdateSize != 0 {
		target.Size = u.Inode.Size
	}
	if u.Mask&InodeUpdateAccessTime != 0 {
		target.AccessTime = u.Inode.AccessTime
	}
	if u.Mask&InodeUpdateModifyTime != 0 {
		target.ModifyTime = u.Inode.ModifyTime
	}
	if u.Mask&InodeUpdateChangeTime != 0 {
		target.ChangeTime = u.Inode.ChangeTime
	}
	if u.Mask&InodeUpdateUID != 0 {
		target.UID = u.Inode.UID
	}
	if u.Mask&InodeUpdateGID != 0 {
		target.GID = u.Inode.GID
	}

	if u.Mask&InodeUpdateMetadata != 0 {
		target.Metadata = make(map[string]string, len(u.Inode.Metadata))
		maps.Copy(target.Metadata, u.Inode.Metadata)
	}

	target.ChangeTime = time.Now()
	return nil
}
