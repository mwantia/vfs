package encrypt

import "github.com/mwantia/vfs/mount/backend"

type VirtualEncryptBackend interface {
	backend.VirtualBackend
}
