package data

import (
	"encoding/json"
	"time"
)

// FileStat represents a low-level representation of FileMetadata.
// It is also used by ObjectStorageBackend to provide basic metadata for objects.
type FileStat struct {
	// Relative key within the backend
	Key string `json:"key"`

	// Hash (for sync/dedup)
	Hash string `json:"hash"`

	// Unix-style mode and permissions
	Mode FileMode `json:"mode"`

	// Size in bytes (0 for directories)
	Size int64 `json:"size"`

	ModifyTime time.Time `json:"modify_time"`
	CreateTime time.Time `json:"create_time"`

	// Content MIME type
	ContentType ContentType `json:"content_type"`

	ETag string `json:"etag"`
}

// ToMetadata converts FileStat into a FileMetadata
func (fs *FileStat) ToMetadata() *Metadata {
	return &Metadata{
		ID:          genMetadataID(),
		Key:         fs.Key,
		Mode:        fs.Mode,
		Size:        fs.Size,
		ModifyTime:  fs.ModifyTime,
		AccessTime:  time.Now(),
		CreateTime:  fs.CreateTime,
		ContentType: fs.ContentType,
		Attributes:  make(map[string]string),
		ETag:        fs.ETag,
	}
}

// Marshal provides JSON serialization for Inode.
func (vs *FileStat) Marshal() ([]byte, error) {
	return json.Marshal(vs)
}

// Unmarshal provides JSON deserialization for Inode.
func (vs *FileStat) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &vs)
}
