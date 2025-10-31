package ephemeral

import (
	"context"
	"sort"
	"strings"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/backend"
)

func (mb *EphemeralBackend) CreateMeta(ctx context.Context, namespace string, meta *data.Metadata) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.createMetaUnsafe(ctx, namespace, meta)
}

func (mb *EphemeralBackend) ReadMeta(ctx context.Context, namespace string, key string) (*data.Metadata, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.readMetaUnsafe(ctx, namespace, key)
}

func (mb *EphemeralBackend) UpdateMeta(ctx context.Context, namespace string, key string, update *data.MetadataUpdate) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.updateMetaUnsafe(ctx, namespace, key, update)
}

func (mb *EphemeralBackend) DeleteMeta(ctx context.Context, namespace string, key string) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.deleteMetaUnsafe(ctx, namespace, key)
}

func (mb *EphemeralBackend) ExistsMeta(ctx context.Context, namespace string, key string) (bool, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.existsMetaUnsafe(ctx, namespace, key)
}

func (mb *EphemeralBackend) QueryMeta(ctx context.Context, namespace string, query *backend.MetadataQuery) (*backend.MetadataQueryResult, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

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
