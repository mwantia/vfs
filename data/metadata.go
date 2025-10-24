package data

import (
	"encoding/json"
	"time"
)

// VirtualFileMetadata represents the core metadata structure.
type VirtualFileMetadata struct {
	// Unique identifier for metadata lookup
	ID string `json:"id"`

	// Relative key within the backend
	Key string `json:"key"`

	// Type of object (file, directory, etc.)
	Type VirtualFileType `json:"type"`

	// Unix-style mode and permissions
	Mode VirtualFileMode `json:"mode"`

	// Size in bytes (0 for directories)
	Size int64 `json:"size"`

	ModifyTime time.Time `json:"modify_time"`
	AccessTime time.Time `json:"access_time"`
	CreateTime time.Time `json:"create_time"`

	// User ownership identity
	UID int64 `json:"uid,omitempty"`

	// Group ownership identity
	GID int64 `json:"gid,omitempty"`

	// Content MIME type
	ContentType string `json:"content_type"`

	// Extended attributes (mount-specific)
	Attributes map[string]string `json:"attributes,omitempty"`

	ETag string `json:"etag"`
}

// Marshal provides JSON serialization for VirtualInode.
func (vfm *VirtualFileMetadata) Marshal() ([]byte, error) {
	return json.Marshal(vfm)
}

// Unmarshal provides JSON deserialization for VirtualInode.
func (vfm *VirtualFileMetadata) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &vfm)
}

// ToStat converts VirtualFileMetadata into a low-level VirtualFileStat.
func (vfm *VirtualFileMetadata) ToStat() *VirtualFileStat {
	return &VirtualFileStat{
		Key:         vfm.Key,
		Type:        vfm.Type,
		Mode:        vfm.Mode,
		Size:        vfm.Size,
		ModifyTime:  vfm.ModifyTime,
		CreateTime:  vfm.CreateTime,
		ContentType: vfm.ContentType,
		ETag:        vfm.ETag,
	}
}

// IsDir returns true if this object is a directory.
func (vfm *VirtualFileMetadata) IsDir() bool {
	return vfm.Type == FileTypeDirectory || vfm.Type == FileTypeMount
}

// IsFile returns true if this object is a regular file.
func (vfm *VirtualFileMetadata) IsFile() bool {
	return vfm.Type == FileTypeFile
}

// IsSymlink returns true if this object is a symbolic link.
func (vfm *VirtualFileMetadata) IsSymlink() bool {
	return vfm.Type == FileTypeSymlink
}

// Clone creates a deep copy of the object info.
func (vfm *VirtualFileMetadata) Clone() *VirtualFileMetadata {
	clone := *vfm
	return &clone
}
