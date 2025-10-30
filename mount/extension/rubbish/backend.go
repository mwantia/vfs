package rubbish

import "github.com/mwantia/vfs/mount/backend"

type RubbishBackendExtension interface {
	backend.Backend
}
