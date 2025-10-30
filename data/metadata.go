package data

import (
	"encoding/json"
	"time"
)

type FileType string

const (
	FileTypeDir       FileType = "directory"
	FileTypeSymlink   FileType = "symlink"
	FileTypeNamedPipe FileType = "namedpipe"
	FileTypeSocket    FileType = "socket"
	FileTypeDevice    FileType = "device"
	FileTypeRegular   FileType = "regular"
)

// Metadata represents the core metadata structure.
type Metadata struct {
	// Unique identifier for metadata lookup
	ID string `json:"id"`

	// Relative key within the backend
	Key string `json:"key"`

	// Unix-style mode and permissions
	// Also used as type of object (file, directory, etc.)
	Mode FileMode `json:"mode"`

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
	ContentType ContentType `json:"content_type"`

	// Extended attributes (mount-specific)
	Attributes map[string]string `json:"attributes,omitempty"`

	ETag string `json:"etag"`
}

// Marshal provides JSON serialization for Inode.
func (m *Metadata) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal provides JSON deserialization for Inode.
func (m *Metadata) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &m)
}

// ToStat converts FileMetadata into a low-level FileStat.
func (m *Metadata) ToStat() *FileStat {
	return &FileStat{
		Key:         m.Key,
		Mode:        m.Mode,
		Size:        m.Size,
		ModifyTime:  m.ModifyTime,
		CreateTime:  m.CreateTime,
		ContentType: m.ContentType,
		ETag:        m.ETag,
	}
}

// GetType returns the filetype defined to this metadata
func (m *Metadata) GetType() FileType {
	switch {
	case m.Mode.IsDir():
		return FileTypeDir
	case m.Mode.IsSymlink():
		return FileTypeSymlink
	case m.Mode.IsNamedPipe():
		return FileTypeNamedPipe
	case m.Mode.IsSocket():
		return FileTypeSocket
	case m.Mode.IsDevice():
		return FileTypeDevice
	default:
		return FileTypeRegular
	}
}

// Clone creates a deep copy of the object info.
func (m *Metadata) Clone() *Metadata {
	clone := *m
	return &clone
}
