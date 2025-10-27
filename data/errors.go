package data

import (
	"errors"
	"sync"
)

// Standard VFS errors that Mount implementations should use.
var (
	// Path resolution errors
	ErrInvalidPath    = errors.New("vfs: invalid path detected")
	ErrNotMounted     = errors.New("vfs: path not mounted")
	ErrAlreadyMounted = errors.New("vfs: path already mounted")
	ErrMountBusy      = errors.New("vfs: mount point busy")
	ErrNestingDenied  = errors.New("vfs: nesting denied by parent mount")

	// Backend errors
	ErrBackendUnsupported  = errors.New("vfs: backend capability unsupported")
	ErrBackendIncompatible = errors.New("vfs: backend incompatible")

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

type Errors struct {
	mu     sync.RWMutex
	errors []error
}

func (e *Errors) Add(err error) {
	if err == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.errors = append(e.errors, err)
}

func (e *Errors) Clear() {
	e.errors = make([]error, 0)
}

func (e *Errors) Errors() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.errors) == 0 {
		return nil
	}

	return errors.Join(e.errors...)
}
