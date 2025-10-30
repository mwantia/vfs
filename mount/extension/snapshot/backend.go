package snapshot

import "github.com/mwantia/vfs/mount/backend"

type SnapshotBackendExtension interface {
	backend.Backend
}
