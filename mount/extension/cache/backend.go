package cache

import "github.com/mwantia/vfs/mount/backend"

type CacheBackendExtension interface {
	backend.Backend
}
