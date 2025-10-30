package data

import (
	"time"

	"github.com/google/uuid"
)

func NewMetadata(key string, fileMode FileMode, size int64) *Metadata {
	now := time.Now()
	id := genMetadataID()

	return &Metadata{
		ID:         id,
		Key:        key,
		Mode:       fileMode,
		Size:       size,
		AccessTime: now,
		ModifyTime: now,
		CreateTime: now,
		Attributes: make(map[string]string),
	}
}

// NewFileMetadata creates new metadata for a regular file.
func NewFileMetadata(key string, size int64, mode FileMode) *Metadata {
	return NewMetadata(key, mode, size)
}

// NewDirectoryMetadata creates a new inode for a directory.
func NewDirectoryMetadata(key string, mode FileMode) *Metadata {
	return NewMetadata(key, mode|ModeDir, 0)
}

// NewSymlinkMetadata creates a new inode for a symbolic link.
func NewSymlinkMetadata(key string, target string) *Metadata {
	metadata := NewMetadata(key, ModeSymlink|0777, 0)
	metadata.Attributes[AttributeSymlinkTarget] = target

	return metadata
}

func genMetadataID() string {
	return uuid.Must(uuid.NewV7()).String()
}
