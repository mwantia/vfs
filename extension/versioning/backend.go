package versioning

import "github.com/mwantia/vfs/backend"

type VirtualVersioningBackend interface {
	backend.VirtualBackend
}
