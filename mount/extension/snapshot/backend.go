package snapshot

import "github.com/mwantia/vfs/mount/backend"

type VirtualSnapshotBackend interface {
	backend.VirtualBackend
}
