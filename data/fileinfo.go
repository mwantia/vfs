package data

import (
	"encoding/json"
	"path/filepath"
	"time"
)

// VirtualFileInfo contains metadata about a virtual object.
// This is the mount-level representation of file/directory information.
type VirtualFileInfo struct {
	// Relative path within the mount
	Path string `json:"path"`

	// Reference to inode (empty if mount doesn't support inodes)
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

// VirtualNodeType identifies the type of object in the filesystem.
type VirtualFileType int

// Node type constants matching common Unix file types.
const (
	NodeTypeFile      VirtualFileType = iota // Regular file
	NodeTypeMount                            // Root Mount
	NodeTypeDirectory                        // Directory
	NodeTypeSymlink                          // Symbolic link
	NodeTypeDevice                           // Device file
	NodeTypeSocket                           // Unix socket
)

// Marshal provides JSON serialization for VirtualNode.
func (vn *VirtualFileInfo) Marshal() ([]byte, error) {
	return json.Marshal(vn)
}

// Unmarshal provides JSON deserialization for VirtualNode.
func (vn *VirtualFileInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &vn)
}

// IsDir returns true if this object is a directory.
func (vn *VirtualFileInfo) IsDir() bool {
	return vn.Type == NodeTypeDirectory
}

// IsFile returns true if this object is a regular file.
func (vn *VirtualFileInfo) IsFile() bool {
	return vn.Type == NodeTypeFile
}

// IsSymlink returns true if this object is a symbolic link.
func (vn *VirtualFileInfo) IsSymlink() bool {
	return vn.Type == NodeTypeSymlink
}

// HasInode returns true if this object has an associated inode.
func (vn *VirtualFileInfo) HasInode() bool {
	return vn.Inode != ""
}

// Clone creates a deep copy of the object info.
func (vn *VirtualFileInfo) Clone() *VirtualFileInfo {
	clone := *vn
	return &clone
}

// CloneWithPath returns a copy with a different path.
// Useful when converting between mount-relative and VFS-absolute paths.
func (vn *VirtualFileInfo) CloneWithPath(path string) *VirtualFileInfo {
	clone := vn.Clone()
	clone.Path = path

	return clone
}

// Name returns the base name of the file or directory.
// This extracts just the filename from the full path.
func (vn *VirtualFileInfo) Name() string {
	return filepath.Base(vn.Path)
}

// Dir returns the directory portion of the path.
// For a file "/foo/bar/file.txt", this returns "/foo/bar".
func (vn *VirtualFileInfo) Dir() string {
	return filepath.Dir(vn.Path)
}

// Ext returns the file extension including the dot.
// For a file "file.txt", this returns ".txt".
// Returns empty string if there is no extension.
func (vn *VirtualFileInfo) Ext() string {
	return filepath.Ext(vn.Path)
}
