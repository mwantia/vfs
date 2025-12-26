package direct

import (
	"sync"

	"github.com/mwantia/vfs/mount/service"
)

var (
	DriverName = "file"
	DriverType = service.ServiceTypeObjectStorage
)

// DirectDriver implements an ObjectStorage-only driver that provides
// direct access to the operating system's filesystem
type DirectDriver struct {
	mu sync.RWMutex

	// Root directory path
	path string
}

// DirectObjectStorageService implements ObjectStorageService
type DirectObjectStorageService struct {
	driver *DirectDriver
}

// DirectBackendConfig holds configuration for the Direct backend
type DirectBackendConfig struct {
	Path string // Root directory path
}
