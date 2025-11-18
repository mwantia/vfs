package ephemeral

import (
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/pkg/mount/backend"
)

func (eb *EphemeralBackend) getMeta(ns, key string) (*data.Metadata, bool) {
	named := backend.NamedKey(ns, key)

	id, exists := eb.keys.Get(named)
	if !exists {
		return nil, false
	}

	meta, exists := eb.metadata[id]
	if !exists {
		return nil, false
	}

	return meta, true
}
