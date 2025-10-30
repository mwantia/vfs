package memory

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/backend"
)

func (mb *MemoryBackend) CreateMeta(ctx context.Context, namespace string, meta *data.Metadata) error {
	// Populate unique ID if not already defined
	if meta.ID == "" {
		meta.ID = uuid.Must(uuid.NewV7()).String()
	}

	// Update CreateTime if
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

func (mb *MemoryBackend) ReadMeta(ctx context.Context, namespace string, key string) (*data.Metadata, error) {
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

func (mb *MemoryBackend) UpdateMeta(ctx context.Context, namespace string, key string, update *data.MetadataUpdate) error {
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

func (mb *MemoryBackend) DeleteMeta(ctx context.Context, namespace string, key string) error {
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

func (mb *MemoryBackend) ExistsMeta(ctx context.Context, namespace string, key string) (bool, error) {
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

func (mb *MemoryBackend) QueryMeta(ctx context.Context, namespace string, query *backend.MetadataQuery) (*backend.MetadataQueryResult, error) {
	var candidates []*data.Metadata

	if query.Delimiter == "/" {
		// Delimiter mode: return only direct children
		if query.Prefix != "" {
			// Non-empty prefix: use pre-computed directory index
			nsPrefix := backend.NamespacedKey(namespace, query.Prefix)
			if children, ok := mb.directories[nsPrefix]; ok {
				for _, nsKey := range children {
					id, _ := mb.keys.Get(nsKey)
					candidates = append(candidates, mb.metadata[id])
				}
			}
		} else {
			// Empty prefix (root): return only top-level entries (no "/" in key)
			// Need to filter by namespace
			nsPrefix := backend.NamespacedKey(namespace, "")
			mb.keys.Scan(func(nsKey string, id string) bool {
				// Check if this key belongs to our namespace
				if namespace != "" && !strings.HasPrefix(nsKey, nsPrefix) {
					return true
				}
				// Extract the actual key (remove namespace prefix)
				key := nsKey
				if namespace != "" {
					key = strings.TrimPrefix(nsKey, namespace+":")
				}
				// Only include top-level entries
				if !strings.Contains(key, "/") {
					candidates = append(candidates, mb.metadata[id])
				}
				return true
			})
		}
	} else {
		// No delimiter: return all entries matching prefix (recursive)
		nsPrefix := backend.NamespacedKey(namespace, query.Prefix)
		mb.keys.Scan(func(nsKey string, id string) bool {
			// Check if this key belongs to our namespace and matches prefix
			if namespace != "" {
				if !strings.HasPrefix(nsKey, namespace+":") {
					return true
				}
			}
			if query.Prefix != "" && !strings.HasPrefix(nsKey, nsPrefix) {
				return true
			}
			candidates = append(candidates, mb.metadata[id])
			return true
		})
	}

	// Apply query filters
	filtered := backend.ApplyFilters(candidates, query)
	// Apply query sorting
	if query.SortBy != "" {
		sort.Slice(filtered, func(i, j int) bool {
			switch query.SortBy {
			case backend.SortByKey:
				if query.SortOrder == backend.SortDesc {
					return filtered[i].Key > filtered[j].Key
				}
				return filtered[i].Key < filtered[j].Key
			case backend.SortBySize:
				if query.SortOrder == backend.SortDesc {
					return filtered[i].Size > filtered[j].Size
				}
				return filtered[i].Size < filtered[j].Size
			case backend.SortByModifyTime:
				if query.SortOrder == backend.SortDesc {
					return filtered[i].ModifyTime.After(filtered[j].ModifyTime)
				}
				return filtered[i].ModifyTime.Before(filtered[j].ModifyTime)
			case backend.SortByCreateTime:
				if query.SortOrder == backend.SortDesc {
					return filtered[i].CreateTime.After(filtered[j].CreateTime)
				}
				return filtered[i].CreateTime.Before(filtered[j].CreateTime)
			default:
				return false
			}
		})
	}

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

	return &backend.MetadataQueryResult{
		Candidates: paginated,
		TotalCount: total,
		Paginating: end < total,
	}, nil
}
