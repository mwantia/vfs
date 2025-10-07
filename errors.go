package vfs

import "errors"

// Standard VFS errors that Mount implementations should use.
var (
	// Path resolution errors
	ErrNotMounted     = errors.New("vfs: path not mounted")
	ErrAlreadyMounted = errors.New("vfs: path already mounted")
	ErrMountBusy      = errors.New("vfs: mount point busy")

	// File operation errors
	ErrNotExist     = errors.New("vfs: file does not exist")
	ErrExist        = errors.New("vfs: file already exists")
	ErrIsDirectory  = errors.New("vfs: is a directory")
	ErrNotDirectory = errors.New("vfs: not a directory")
	ErrPermission   = errors.New("vfs: permission denied")
	ErrReadOnly     = errors.New("vfs: read-only filesystem")

	// I/O errors
	ErrClosed  = errors.New("vfs: file already closed")
	ErrInvalid = errors.New("vfs: invalid argument")
	ErrInUse   = errors.New("vfs: file already in use")
)
