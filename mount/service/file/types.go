package file

import (
	"sync"

	"github.com/mwantia/vfs/mount/service"
)

var (
	DriverName = "file"
	DriverType = service.ServiceTypeObjectStorage
)

// FileDriver implements an ObjectStorage-only driver that provides
// direct access to the operating system's filesystem
type FileDriver struct {
	mu sync.RWMutex

	// Root directory path
	path string
}

// FileObjectStorageService implements ObjectStorageService
type FileObjectStorageService struct {
	driver *FileDriver
}

// FileBackendConfig holds configuration for the File backend
type FileBackendConfig struct {
	Path string // Root directory path
}
