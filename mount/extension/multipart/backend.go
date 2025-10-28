package multipart

import "github.com/mwantia/vfs/mount/backend"

type VirtualMultipartBackend interface {
	backend.VirtualBackend
}
