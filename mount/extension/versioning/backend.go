package versioning

import "github.com/mwantia/vfs/mount/backend"

type VersioningBackendExtension interface {
	backend.Backend
}
