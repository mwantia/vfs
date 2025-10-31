package direct

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

type DirectBackend struct {
	mu   sync.RWMutex
	path string
}

func NewDirectBackend(path string) (*DirectBackend, error) {
	return &DirectBackend{
		path: filepath.Clean(path),
	}, nil
}

// Returns the identifier name defined for this backend
func (*DirectBackend) Name() string {
	return "direct"
}

// Open is part of the lifecycle behavious and gets called when opening this backend.
func (db *DirectBackend) Open(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Verify the root directory exists
	info, err := os.Stat(db.path)
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
func (db *DirectBackend) Close(ctx context.Context) error {
	// The underlying filesystem persists independently
	return nil
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (db *DirectBackend) GetCapabilities() *backend.BackendCapabilities {
	return &backend.BackendCapabilities{
		Capabilities: []backend.BackendCapability{
			backend.CapabilityObjectStorage,
		},
		// Ephemeral filesystem limits vary by OS/filesystem, but we set a practical limit
		// of 10GB for typical VFS use cases. Adjust as needed for your requirements.
		MaxObjectSize: 10737418240, // 10 GB
	}
}

// resolvePath joins the backend path with the relative path.
func (db *DirectBackend) resolvePath(path string) string {
	return filepath.Join(db.path, filepath.Clean(path))
}

// toVirtualFileStat converts os.FileInfo to a VirtualFileStat.
func (db *DirectBackend) toFileStat(key string, fileInfo os.FileInfo) *data.FileStat {
	virtMode := data.FileMode(fileInfo.Mode().Perm())
	if fileInfo.IsDir() {
		virtMode |= data.ModeDir
	}

	return &data.FileStat{
		Key:  key,
		Size: fileInfo.Size(),
		Mode: virtMode,

		ModifyTime:  fileInfo.ModTime(),
		ContentType: data.GetMIMEType(fileInfo.Name()),
	}
}
