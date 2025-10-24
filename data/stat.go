package data

import (
	"encoding/json"
	"time"
)

// VirtualFileStat represents a low-level representation of VirtualFileMetadata.
// It is also used by VirtualObjectStorageBackend to provide basic metadata for objects.
type VirtualFileStat struct {
	// Relative key within the backend
	Key string `json:"key"`

	// Hash (for sync/dedup)
	Hash string `json:"hash"`

	// Unix-style mode and permissions
	Mode VirtualFileMode `json:"mode"`

	// Size in bytes (0 for directories)
	Size int64 `json:"size"`

	ModifyTime time.Time `json:"modify_time"`
	CreateTime time.Time `json:"create_time"`

	// Content MIME type
	ContentType string `json:"content_type"`

	ETag string `json:"etag"`
}

// ToMetadata converts VirtualFileStat into a VirtualFileMetadata
func (vfs *VirtualFileStat) ToMetadata() *VirtualFileMetadata {
	return &VirtualFileMetadata{
		ID:          genMetadataID(),
		Key:         vfs.Key,
		Mode:        vfs.Mode,
		Size:        vfs.Size,
		ModifyTime:  vfs.ModifyTime,
		AccessTime:  time.Now(),
		CreateTime:  vfs.CreateTime,
		ContentType: vfs.ContentType,
		Attributes:  make(map[string]string),
		ETag:        vfs.ETag,
	}
}

// Marshal provides JSON serialization for VirtualInode.
func (vs *VirtualFileStat) Marshal() ([]byte, error) {
	return json.Marshal(vs)
}

// Unmarshal provides JSON deserialization for VirtualInode.
func (vs *VirtualFileStat) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &vs)
}
