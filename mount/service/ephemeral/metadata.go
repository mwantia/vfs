package ephemeral

import (
	"strings"
	"time"

	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

func (s *EphemeralMetadataService) GetLifecycle() service.Lifecycle {
	return s.driver
}

func (ems *EphemeralMetadataService) CreateMeta(traversal context.TraversalContext, ns string, meta *data.Metadata) error {
	ems.driver.mu.Lock()
	defer ems.driver.mu.Unlock()

	named := service.NamedKey(ns, meta.Key, ":")

	// Check if key already exists
	if _, exists := ems.driver.keys.Get(named); exists {
		return data.ErrExist
	}

	// Enforce timestamp correctness
	now := time.Now()
	if meta.CreateTime.IsZero() {
		meta.CreateTime = now
	}
	meta.ModifyTime = now
	meta.AccessTime = now

	// Store metadata
	ems.driver.keys.Set(named, meta.ID)
	ems.driver.metadata[meta.ID] = meta

	// Increment reference count for this metadata ID
	ems.driver.refCount[meta.ID]++

	// Update directory index for fast lookups
	if idx := strings.LastIndex(meta.Key, "/"); idx >= 0 {
		// Parent directory path (everything before last /)
		dir := service.NamedKey(ns, meta.Key[:idx+1], ":")
		// Add this key to parent's children list
		if ems.driver.dirs[dir] == nil {
			ems.driver.dirs[dir] = make([]string, 0)
		}
		ems.driver.dirs[dir] = append(ems.driver.dirs[dir], named)
	}

	return nil
}

func (ems *EphemeralMetadataService) ReadMeta(traversal context.TraversalContext, ns, key string) (*data.Metadata, error) {
	ems.driver.mu.RLock()
	defer ems.driver.mu.RUnlock()

	meta, exists := ems.getMeta(ns, key)
	if !exists {
		return nil, data.ErrNotExist
	}

	// Update access time (acceptable race condition for performance)
	// This is safe to do under a read lock since we're only updating a timestamp
	meta.AccessTime = time.Now()
	return meta, nil
}

func (ems *EphemeralMetadataService) UpdateMeta(traversal context.TraversalContext, ns, key string, update *data.MetadataUpdate) error {
	ems.driver.mu.Lock()
	defer ems.driver.mu.Unlock()

	meta, exists := ems.getMeta(ns, key)
	if !exists {
		return data.ErrNotExist
	}

	meta.ModifyTime = time.Now()
	if _, err := update.Apply(meta); err != nil {
		return err
	}

	ems.driver.metadata[meta.ID] = meta
	return nil
}

func (ems *EphemeralMetadataService) DeleteMeta(traversal context.TraversalContext, ns, key string) error {
	ems.driver.mu.Lock()
	defer ems.driver.mu.Unlock()

	named := service.NamedKey(ns, key, ":")
	id, exists := ems.driver.keys.Get(named)
	if !exists {
		return data.ErrNotExist
	}

	// Delete the key-to-ID mapping
	if _, ok := ems.driver.keys.Delete(named); ok {
		// Decrement reference count
		ems.driver.refCount[id]--

		// If no more references exist, delete the metadata
		if ems.driver.refCount[id] <= 0 {
			delete(ems.driver.metadata, id)
			delete(ems.driver.refCount, id)
		}

		// Update directory index - remove this key from parent's children
		if idx := strings.LastIndex(key, "/"); idx >= 0 {
			dir := service.NamedKey(ns, key[:idx+1], ":")
			if children, ok := ems.driver.dirs[dir]; ok {
				// Find and remove this key from children list
				for i, child := range children {
					if child == named {
						// Remove by swapping with last element and truncating
						ems.driver.dirs[dir] = append(children[:i], children[i+1:]...)
						break
					}
				}
				// Clean up empty directory entries
				if len(ems.driver.dirs[dir]) == 0 {
					delete(ems.driver.dirs, dir)
				}
			}
		}
	}

	return nil
}

func (ems *EphemeralMetadataService) ExistsMeta(traversal context.TraversalContext, ns, key string) (bool, error) {
	ems.driver.mu.RLock()
	defer ems.driver.mu.RUnlock()

	_, exists := ems.getMeta(ns, key)
	return exists, nil
}

func (ems *EphemeralMetadataService) QueryMeta(traversal context.TraversalContext, ns string, query *service.Query) (*service.QueryPagination, error) {
	ems.driver.mu.RLock()
	defer ems.driver.mu.RUnlock()

	var candidates []*data.Metadata

	if query.Delimiter == "/" {
		// Delimiter mode: return only direct children
		if query.Prefix != "" {
			// Non-empty prefix: use pre-computed directory index
			nsPrefix := service.NamedKey(ns, query.Prefix, ":")
			if children, ok := ems.driver.dirs[nsPrefix]; ok {
				for _, nsKey := range children {
					id, _ := ems.driver.keys.Get(nsKey)
					candidates = append(candidates, ems.driver.metadata[id])
				}
			}
		} else {
			// Empty prefix (root): return only top-level entries (no "/" in key)
			// Need to filter by namespace
			nsPrefix := service.NamedKey(ns, "", ":")
			ems.driver.keys.Scan(func(nsKey string, id string) bool {
				// Check if this key belongs to our namespace
				if ns != "" && !strings.HasPrefix(nsKey, nsPrefix) {
					return true
				}
				// Extract the actual key (remove namespace prefix)
				key := nsKey
				if ns != "" {
					key = strings.TrimPrefix(nsKey, ns+":")
				}
				// Only include top-level entries
				if !strings.Contains(key, "/") {
					candidates = append(candidates, ems.driver.metadata[id])
				}

				return true
			})
		}
	} else {
		// No delimiter: return all entries matching prefix (recursive)
		nsPrefix := service.NamedKey(ns, query.Prefix, ":")
		ems.driver.keys.Scan(func(nsKey string, id string) bool {
			// Check if this key belongs to our namespace and matches prefix
			if ns != "" {
				if !strings.HasPrefix(nsKey, ns+":") {
					return true
				}
			}
			if query.Prefix != "" && !strings.HasPrefix(nsKey, nsPrefix) {
				return true
			}
			candidates = append(candidates, ems.driver.metadata[id])

			return true
		})
	}

	// Apply query filters and sorting
	filtered := service.ApplyFilters(candidates, query)

	// Apply pagination
	total := len(filtered)
	start := query.Offset
	end := total

	if query.Limit > 0 {
		end = min(start+query.Limit, total)
	}

	// Ensure valid range
	if start > total {
		start = total
	}

	paginated := filtered[start:end]

	// Update access time for all returned items (they were accessed by this query)
	now := time.Now()
	for _, meta := range paginated {
		meta.AccessTime = now
	}

	return &service.QueryPagination{
		Candidates: paginated,
		TotalCount: total,
		Paginating: end < total,
	}, nil
}

func (ems *EphemeralMetadataService) getMeta(ns, key string) (*data.Metadata, bool) {
	named := service.NamedKey(ns, key, ":")

	id, exists := ems.driver.keys.Get(named)
	if !exists {
		return nil, false
	}

	meta, exists := ems.driver.metadata[id]
	if !exists {
		return nil, false
	}

	return meta, true
}
