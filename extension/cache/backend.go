package cache

import "github.com/mwantia/vfs/backend"

type VirtualCacheBackend interface {
	backend.VirtualBackend
}
