package data

import (
	"encoding/json"
	"time"
)

// VirtualStat is a lighter-weight version of VirtualNode and VirtualInode that only includes
// stat-level information. Useful for operations that don't need full inode details.
type VirtualStat struct {
	// Identity - unique identifier
	Inode string `json:"inode"`

	// Type of object (file, directory, etc.)
	Type VirtualFileType `json:"type"`

	// Unix-style mode and permissions
	Mode VirtualFileMode `json:"mode"`

	// Size in bytes (0 for directories)
	Size int64 `json:"size"`

	// Last modification time
	ModifyTime time.Time `json:"modify_time"`
}

// ToStat converts a full VirtualInode to a lightweight stat structure.
func (vi *VirtualInode) ToStat(path string) *VirtualStat {
	return &VirtualStat{
		Inode:      vi.ID,
		Type:       vi.Type,
		Mode:       vi.Mode,
		Size:       vi.Size,
		ModifyTime: vi.ModifyTime,
	}
}

// ToStat converts a full VirtualNode to a lightweight stat structure.
func (vi *VirtualFileInfo) ToStat(path string) *VirtualStat {
	return &VirtualStat{
		Inode:      vi.Inode,
		Type:       vi.Type,
		Mode:       vi.Mode,
		Size:       vi.Size,
		ModifyTime: vi.ModifyTime,
	}
}

// Marshal provides JSON serialization for VirtualInode.
func (vs *VirtualStat) Marshal() ([]byte, error) {
	return json.Marshal(vs)
}

// Unmarshal provides JSON deserialization for VirtualInode.
func (vs *VirtualStat) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &vs)
}

// IsDir returns true if this object is a directory.
func (vs *VirtualStat) IsDir() bool {
	return vs.Type == NodeTypeDirectory
}

// IsFile returns true if this object is a regular file.
func (vs *VirtualStat) IsFile() bool {
	return vs.Type == NodeTypeFile
}

// IsSymlink returns true if this object is a symbolic link.
func (vs *VirtualStat) IsSymlink() bool {
	return vs.Type == NodeTypeSymlink
}

// HasInode returns true if this object has an associated inode.
func (vs *VirtualStat) HasInode() bool {
	return vs.Inode != ""
}
