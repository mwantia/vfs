package ephemeral

import (
	"strings"
	"time"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/pkg/mount/backend"
)

// This is a unsafe call that doesn't acquire or check locks.
func (eb *EphemeralBackend) unsafeCreateMeta(ns string, meta *data.Metadata) error {
	named := backend.NamedKey(ns, meta.Key)

	eb.keys.Set(named, meta.ID)
	eb.metadata[meta.ID] = meta

	// Update directory index for fast lookups and extract parent directory from key
	if idx := strings.LastIndex(meta.Key, "/"); idx >= 0 {
		// Parent directory path (everything before last /)
		dir := backend.NamedKey(ns, meta.Key[:idx+1])
		// Add this key to parent's children list
		if eb.dirs[dir] == nil {
			eb.dirs[dir] = make([]string, 0)
		}
		eb.dirs[dir] = append(eb.dirs[dir], named)
	}

	return nil
}

func (eb *EphemeralBackend) unsafeReadMeta(ns, key string) (*data.Metadata, error) {
	meta, exists := eb.getMeta(ns, key)
	if !exists {
		return nil, data.ErrNotExist
	}

	meta.AccessTime = time.Now()
	return meta, nil
}

func (eb *EphemeralBackend) unsafeUpdateMeta(ns, key string, update *data.MetadataUpdate) error {
	meta, exists := eb.getMeta(ns, key)
	if !exists {
		return data.ErrNotExist
	}

	meta.ModifyTime = time.Now()
	if _, err := update.Apply(meta); err != nil {
		return err
	}

	eb.metadata[meta.ID] = meta
	return nil
}

func (eb *EphemeralBackend) unsafeDeleteMeta(ns, key string) error {
	named := backend.NamedKey(ns, key)
	id, exists := eb.keys.Get(named)
	if !exists {
		return data.ErrNotExist
	}

	if _, ok := eb.keys.Delete(named); ok {
		hardLinks := false

		eb.keys.Scan(func(key, value string) bool {
			if value == id {
				hardLinks = true
				return false
			}

			return true
		})
		if !hardLinks {
			delete(eb.metadata, id)
		}
		// Update directory index - remove this key from parent's children
		if idx := strings.LastIndex(key, "/"); idx >= 0 {
			dir := backend.NamedKey(ns, key[:idx+1])
			if children, ok := eb.dirs[dir]; ok {
				// Find and remove this key from children list
				for i, child := range children {
					if child == named {
						// Remove by swapping with last element and truncating
						eb.dirs[dir] = append(children[:i], children[i+1:]...)
						break
					}
				}
				// Clean up empty directory entries
				if len(eb.dirs[dir]) == 0 {
					delete(eb.dirs, dir)
				}
			}
		}
	}

	return nil
}

func (eb *EphemeralBackend) unsafeExistsMeta(ns, key string) (bool, error) {
	_, exists := eb.getMeta(ns, key)
	if exists {
		return true, nil
	}

	return false, data.ErrNotExist
}
