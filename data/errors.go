package data

import (
	"errors"
)

// Standard VFS errors that Mount implementations should use.
var (
	// Mount lifecycle errors
	ErrMountFailed       = errors.New("vfs: mount initialization failed")
	ErrUnmountFailed     = errors.New("vfs: unmount cleanup failed")
	ErrSelfReferential   = errors.New("vfs: self-referential mount not allowed")
	ErrCircularReference = errors.New("vfs: circular mount reference detected")

	// File operation errors
	ErrNotExist          = errors.New("vfs: file does not exist")
	ErrExist             = errors.New("vfs: file already exists")
	ErrIsDirectory       = errors.New("vfs: is a directory")
	ErrNotDirectory      = errors.New("vfs: not a directory")
	ErrPermission        = errors.New("vfs: permission denied")
	ErrReadOnly          = errors.New("vfs: read-only filesystem")
	ErrDirectoryNotEmpty = errors.New("vfs: directory not empty")

	// I/O errors
	ErrClosed  = errors.New("vfs: file already closed")
	ErrBusy    = errors.New("vfs: file is busy")
	ErrInvalid = errors.New("vfs: invalid argument")
	ErrInUse   = errors.New("vfs: file already in use")
)
