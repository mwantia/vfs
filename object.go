package vfs

import (
	"io"
	"time"
)

// VirtualObject represents a file, directory, or other object within a VFS mount.
// It combines metadata (Info) with the actual content (Data) for the object.
type VirtualObject struct {
	Info VirtualObjectInfo // Metadata about the object
	Data io.Reader         // Content data (nil for directories)
}

// VirtualObjectInfo contains metadata about a virtual object.
// This is the mount-level representation of file/directory information.
type VirtualObjectInfo struct {
	Path     string            // Relative path within the mount
	Name     string            // Base name of the object
	Type     VirtualObjectType // Type of object (file, directory, etc.)
	Size     int64             // Size in bytes (0 for directories)
	Mode     VirtualFileMode   // Unix-style mode and permissions
	ModTime  time.Time         // Last modification time
	Metadata map[string]string // Extended metadata (mount-specific)
}

// VirtualObjectType identifies the type of object in the filesystem.
type VirtualObjectType int

// Object type constants matching common Unix file types.
const (
	ObjectTypeFile      VirtualObjectType = iota // Regular file
	ObjectTypeDirectory                          // Directory
	ObjectTypeSymlink                            // Symbolic link
	ObjectTypeDevice                             // Device file
	ObjectTypeSocket                             // Unix socket
)
