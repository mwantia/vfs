package versioning

import "github.com/mwantia/vfs/mount/backend"

type VirtualVersioningBackend interface {
	backend.VirtualBackend
}
