package encrypt

import "github.com/mwantia/vfs/mount/backend"

type EncryptBackendExtension interface {
	backend.Backend
}
