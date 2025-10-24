package data

import (
	"time"

	"github.com/google/uuid"
)

func NewMetadata(key string, fileType VirtualFileType, fileMode VirtualFileMode, size int64) *VirtualFileMetadata {
	now := time.Now()
	id := genMetadataID()

	return &VirtualFileMetadata{
		ID:         id,
		Key:        key,
		Type:       fileType,
		Mode:       fileMode,
		Size:       size,
		AccessTime: now,
		ModifyTime: now,
		CreateTime: now,
		Attributes: make(map[string]string),
	}
}

// NewFileMetadata creates new metadata for a regular file.
func NewFileMetadata(key string, size int64, mode VirtualFileMode) *VirtualFileMetadata {
	return NewMetadata(key, FileTypeFile, mode, size)
}

// NewMountMetadata creates a new inode for a directory.
func NewMountMetadata() *VirtualFileMetadata {
	return NewMetadata("", FileTypeMount, ModeDir|0777, 0)
}

// NewDirectoryMetadata creates a new inode for a directory.
func NewDirectoryMetadata(key string, mode VirtualFileMode) *VirtualFileMetadata {
	return NewMetadata(key, FileTypeDirectory, mode|ModeDir, 0)
}

// NewSymlinkMetadata creates a new inode for a symbolic link.
func NewSymlinkMetadata(key string, target string) *VirtualFileMetadata {
	metadata := NewMetadata(key, FileTypeSymlink, ModeSymlink|0777, 0)
	metadata.Attributes[AttributeSymlinkTarget] = target

	return metadata
}

func genMetadataID() string {
	return uuid.Must(uuid.NewV7()).String()
}
