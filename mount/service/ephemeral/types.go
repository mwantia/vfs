package ephemeral

import (
	"sync"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
	"github.com/tidwall/btree"
)

var (
	DriverName = "ephemeral"
	DriverType = service.ServiceTypeMonolith
)

type EphemeralMonolithDriver struct {
	mu sync.RWMutex

	// Metadata
	keys     *btree.Map[string, string]
	dirs     map[string][]string
	metadata map[string]*data.Metadata
	refCount map[string]int

	// Object Storage
	stats map[string]*data.FileStat
	datas map[string][]byte
}

type EphemeralMetadataService struct {
	driver *EphemeralMonolithDriver
}

type EphemeralObjectStorageService struct {
	driver *EphemeralMonolithDriver
}

type EphemeralMountExtensionService struct {
	driver *EphemeralMonolithDriver
}
