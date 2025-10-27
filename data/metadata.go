package data

import (
	"encoding/json"
	"time"
)

type VirtualFileType string

const (
	FileTypeDir       VirtualFileType = "directory"
	FileTypeSymlink   VirtualFileType = "symlink"
	FileTypeNamedPipe VirtualFileType = "namedpipe"
	FileTypeSocket    VirtualFileType = "socket"
	FileTypeDevice    VirtualFileType = "device"
	FileTypeRegular   VirtualFileType = "regular"
)

// VirtualFileMetadata represents the core metadata structure.
type VirtualFileMetadata struct {
	// Unique identifier for metadata lookup
	ID string `json:"id"`

	// Relative key within the backend
	Key string `json:"key"`

	// Unix-style mode and permissions
	// Also used as type of object (file, directory, etc.)
	Mode VirtualFileMode `json:"mode"`

	// Size in bytes (0 for directories)
	Size int64 `json:"size"`

	AccessTime time.Time `json:"access_time"`
	ModifyTime time.Time `json:"modify_time"`
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
		Mode:        vfm.Mode,
		Size:        vfm.Size,
		ModifyTime:  vfm.ModifyTime,
		CreateTime:  vfm.CreateTime,
		ContentType: vfm.ContentType,
		ETag:        vfm.ETag,
	}
}

// GetType returns the filetype defined to this metadata
func (vfm *VirtualFileMetadata) GetType() VirtualFileType {
	switch {
	case vfm.Mode.IsDir():
		return FileTypeDir
	case vfm.Mode.IsSymlink():
		return FileTypeSymlink
	case vfm.Mode.IsNamedPipe():
		return FileTypeNamedPipe
	case vfm.Mode.IsSocket():
		return FileTypeSocket
	case vfm.Mode.IsDevice():
		return FileTypeDevice
	default:
		return FileTypeRegular
	}
}

// Clone creates a deep copy of the object info.
func (vfm *VirtualFileMetadata) Clone() *VirtualFileMetadata {
	clone := *vfm
	return &clone
}
