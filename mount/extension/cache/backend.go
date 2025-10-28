package cache

import "github.com/mwantia/vfs/mount/backend"

type VirtualCacheBackend interface {
	backend.VirtualBackend
}
