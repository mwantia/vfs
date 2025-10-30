package multipart

import "github.com/mwantia/vfs/mount/backend"

type MultipartBackendExtension interface {
	backend.Backend
}
