package mount

import (
	"sync"
	"time"

	"github.com/mwantia/vfs/mount/service"
)

type Mount struct {
	mu         sync.RWMutex
	fstab      map[string]*Mount
	uris       []string
	createTime time.Time

	MountPoint string
	Options    *MountOptions

	ObjectStorage service.ObjectStorageService
	Metadata      service.MetadataService
	Extensions    map[service.ServiceExtension]service.Service
}

type MountOptions struct {
	Namespace  string
	PathPrefix string
	Cascading  bool
	IsReadOnly bool
}
