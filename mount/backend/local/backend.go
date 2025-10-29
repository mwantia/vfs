package local

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/backend"
)

type LocalBackend struct {
	mu   sync.RWMutex
	path string
}

func NewLocalBackend(path string) *LocalBackend {
	return &LocalBackend{
		path: filepath.Clean(path),
	}
}

// Returns the identifier name defined for this backend
func (*LocalBackend) Name() string {
	return "local"
}

// Open is part of the lifecycle behavious and gets called when opening this backend.
func (lb *LocalBackend) Open(ctx context.Context) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Verify the root directory exists
	info, err := os.Stat(lb.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return data.ErrMountFailed
		}
		if errors.Is(err, fs.ErrPermission) {
			return data.ErrPermission
		}

		return data.ErrMountFailed
	}

	// Ensure the root is a directory
	if !info.IsDir() {
		return data.ErrNotDirectory
	}

	return nil
}

// Close is part of the lifecycle behaviour and gets called when closing this backend.
func (lb *LocalBackend) Close(ctx context.Context) error {
	// The underlying filesystem persists independently
	return nil
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (lb *LocalBackend) GetCapabilities() *backend.VirtualBackendCapabilities {
	return &backend.VirtualBackendCapabilities{
		Capabilities: []backend.VirtualBackendCapability{
			backend.CapabilityObjectStorage,
		},
		// Local filesystem limits vary by OS/filesystem, but we set a practical limit
		// of 10GB for typical VFS use cases. Adjust as needed for your requirements.
		MaxObjectSize: 10737418240, // 10 GB
	}
}

// resolvePath joins the backend path with the relative path.
func (lb *LocalBackend) resolvePath(path string) string {
	return filepath.Join(lb.path, filepath.Clean(path))
}

// toVirtualFileStat converts os.FileInfo to a VirtualFileStat.
func (lb *LocalBackend) toVirtualFileStat(key string, fileInfo os.FileInfo) *data.VirtualFileStat {
	virtMode := data.VirtualFileMode(fileInfo.Mode().Perm())
	if fileInfo.IsDir() {
		virtMode |= data.ModeDir
	}

	return &data.VirtualFileStat{
		Key:  key,
		Size: fileInfo.Size(),
		Mode: virtMode,

		ModifyTime:  fileInfo.ModTime(),
		ContentType: data.GetMIMEType(fileInfo.Name()),
	}
}
