package ephemeral

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/backend"
)

// This file contains internal "unsafe" methods that perform operations without acquiring locks.
// These methods MUST only be called when the caller already holds the appropriate lock.
// They are used by both public API methods (which acquire locks) and internal methods
// (like storage operations that already hold locks).

// createMetaUnsafe creates metadata without acquiring locks.
// MUST be called while holding a write lock.
func (mb *EphemeralBackend) createMetaUnsafe(ctx context.Context, namespace string, meta *data.Metadata) error {
	// Populate unique ID if not already defined
	if meta.ID == "" {
		meta.ID = uuid.Must(uuid.NewV7()).String()
	}

	// Update CreateTime if not set
	if meta.CreateTime.IsZero() {
		meta.CreateTime = time.Now()
	}

	nsKey := backend.NamespacedKey(namespace, meta.Key)
	mb.keys.Set(nsKey, meta.ID)
	mb.metadata[meta.ID] = meta

	// Update directory index for fast lookups
	// Extract parent directory from key
	if idx := strings.LastIndex(meta.Key, "/"); idx >= 0 {
		// Parent directory path (everything before last /)
		parentDir := meta.Key[:idx+1]
		nsParentDir := backend.NamespacedKey(namespace, parentDir)
		// Add this key to parent's children list
		if mb.directories[nsParentDir] == nil {
			mb.directories[nsParentDir] = make([]string, 0)
		}
		mb.directories[nsParentDir] = append(mb.directories[nsParentDir], nsKey)
	}

	return nil
}

// readMetaUnsafe reads metadata without acquiring locks.
// MUST be called while holding at least a read lock.
func (mb *EphemeralBackend) readMetaUnsafe(ctx context.Context, namespace string, key string) (*data.Metadata, error) {
	nsKey := backend.NamespacedKey(namespace, key)
	id, exists := mb.keys.Get(nsKey)
	if !exists {
		return nil, data.ErrNotExist
	}

	meta, exists := mb.metadata[id]
	if !exists {
		return nil, data.ErrNotExist
	}

	// Update access time (acceptable race condition for performance)
	meta.AccessTime = time.Now()
	return meta, nil
}

// updateMetaUnsafe updates metadata without acquiring locks.
// MUST be called while holding a write lock.
func (mb *EphemeralBackend) updateMetaUnsafe(ctx context.Context, namespace string, key string, update *data.MetadataUpdate) error {
	nsKey := backend.NamespacedKey(namespace, key)
	id, exists := mb.keys.Get(nsKey)
	if !exists {
		return data.ErrNotExist
	}

	meta, exists := mb.metadata[id]
	if !exists {
		return data.ErrNotExist
	}

	meta.ModifyTime = time.Now()
	if _, err := update.Apply(meta); err != nil {
		return err
	}

	mb.metadata[id] = meta
	return nil
}

// deleteMetaUnsafe deletes metadata without acquiring locks.
// MUST be called while holding a write lock.
func (mb *EphemeralBackend) deleteMetaUnsafe(ctx context.Context, namespace string, key string) error {
	nsKey := backend.NamespacedKey(namespace, key)
	id, exists := mb.keys.Get(nsKey)
	if !exists {
		return data.ErrNotExist
	}

	if _, ok := mb.keys.Delete(nsKey); ok {
		hardLinks := false
		mb.keys.Scan(func(key, value string) bool {
			if value == id {
				hardLinks = true
				return false
			}

			return true
		})
		if !hardLinks {
			delete(mb.datas, id)
			delete(mb.metadata, id)
		}

		// Update directory index - remove this key from parent's children
		if idx := strings.LastIndex(key, "/"); idx >= 0 {
			parentDir := key[:idx+1]
			nsParentDir := backend.NamespacedKey(namespace, parentDir)
			if children, ok := mb.directories[nsParentDir]; ok {
				// Find and remove this key from children list
				for i, child := range children {
					if child == nsKey {
						// Remove by swapping with last element and truncating
						mb.directories[nsParentDir] = append(children[:i], children[i+1:]...)
						break
					}
				}
				// Clean up empty directory entries
				if len(mb.directories[nsParentDir]) == 0 {
					delete(mb.directories, nsParentDir)
				}
			}
		}
	}

	return nil
}

// existsMetaUnsafe checks if metadata exists without acquiring locks.
// MUST be called while holding at least a read lock.
func (mb *EphemeralBackend) existsMetaUnsafe(ctx context.Context, namespace string, key string) (bool, error) {
	nsKey := backend.NamespacedKey(namespace, key)
	id, exists := mb.keys.Get(nsKey)
	if !exists {
		return false, data.ErrNotExist
	}

	_, exists = mb.metadata[id]
	if !exists {
		return false, data.ErrNotExist
	}

	return true, nil
}
