package data

import "time"

// NewFileInode creates a new inode for a regular file.
func NewFileInode(id string, size int64, mode VirtualFileMode) *VirtualInode {
	now := time.Now()

	return &VirtualInode{
		ID:         id,
		Type:       NodeTypeFile,
		Mode:       mode,
		Size:       size,
		AccessTime: now,
		ModifyTime: now,
		ChangeTime: now,
		CreateTime: now,
		Metadata:   make(map[string]string),
	}
}

// NewDirectoryInode creates a new inode for a directory.
func NewDirectoryInode(id string, mode VirtualFileMode) *VirtualInode {
	now := time.Now()

	return &VirtualInode{
		ID:         id,
		Type:       NodeTypeDirectory,
		Mode:       mode | ModeDir,
		Size:       0,
		AccessTime: now,
		ModifyTime: now,
		ChangeTime: now,
		CreateTime: now,
		Metadata:   make(map[string]string),
	}
}

// NewSymlinkInode creates a new inode for a symbolic link.
func NewSymlinkInode(id string, target string) *VirtualInode {
	now := time.Now()

	return &VirtualInode{
		ID:   id,
		Type: NodeTypeSymlink,
		// Symlinks typically have 0777
		Mode:       ModeSymlink | 0777,
		Size:       int64(len(target)),
		AccessTime: now,
		ModifyTime: now,
		ChangeTime: now,
		CreateTime: now,
		Metadata:   make(map[string]string),
	}
}
