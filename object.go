package vfs

import (
	"io"
	"time"
)

// VirtualObject represents an object within the VFS mount
type VirtualObject struct {
	Info VirtualObjectInfo
	Data io.Reader
}

// VirtualObjectInfo contains object metadata
type VirtualObjectInfo struct {
	Path     string
	Name     string
	Type     VirtualObjectType
	Size     int64
	Mode     VirtualFileMode
	ModTime  time.Time
	Metadata map[string]string
}

// VirtualObjectType identifies what kind of object this is
type VirtualObjectType int

const (
	ObjectTypeFile VirtualObjectType = iota
	ObjectTypeDirectory
	ObjectTypeSymlink
	ObjectTypeDevice
	ObjectTypeSocket
)
