package ephemeral

import (
	"strings"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/pkg/context"
	"github.com/mwantia/vfs/pkg/mount/backend"
)

func (eb *EphemeralBackend) CreateMeta(ctx context.TraversalContext, ns string, meta *data.Metadata) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	return eb.unsafeCreateMeta(ns, meta)
}

func (eb *EphemeralBackend) ReadMeta(ctx context.TraversalContext, ns, key string) (*data.Metadata, error) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	return eb.unsafeReadMeta(ns, key)
}

func (eb *EphemeralBackend) UpdateMeta(ctx context.TraversalContext, ns, key string, update *data.MetadataUpdate) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	return eb.unsafeUpdateMeta(ns, key, update)
}

func (eb *EphemeralBackend) DeleteMeta(ctx context.TraversalContext, ns, key string) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	return eb.unsafeDeleteMeta(ns, key)
}

func (eb *EphemeralBackend) ExistsMeta(ctx context.TraversalContext, ns, key string) (bool, error) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	return eb.unsafeExistsMeta(ns, key)
}

func (eb *EphemeralBackend) QueryMeta(ctx context.TraversalContext, ns string, query *backend.Query) (*backend.QueryResult, error) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	var candidates []*data.Metadata

	if query.Delimiter == "/" {
		// Delimiter mode: return only direct children
		if query.Prefix != "" {
			// Non-empty prefix: use pre-computed directory index
			nsPrefix := backend.NamedKey(ns, query.Prefix)
			if children, ok := eb.dirs[nsPrefix]; ok {
				for _, nsKey := range children {
					id, _ := eb.keys.Get(nsKey)
					candidates = append(candidates, eb.metadata[id])
				}
			}
		} else {
			// Empty prefix (root): return only top-level entries (no "/" in key)
			// Need to filter by namespace
			nsPrefix := backend.NamedKey(ns, "")
			eb.keys.Scan(func(nsKey string, id string) bool {
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
					candidates = append(candidates, eb.metadata[id])
				}

				return true
			})
		}
	} else {
		// No delimiter: return all entries matching prefix (recursive)
		nsPrefix := backend.NamedKey(ns, query.Prefix)
		eb.keys.Scan(func(nsKey string, id string) bool {
			// Check if this key belongs to our namespace and matches prefix
			if ns != "" {
				if !strings.HasPrefix(nsKey, ns+":") {
					return true
				}
			}
			if query.Prefix != "" && !strings.HasPrefix(nsKey, nsPrefix) {
				return true
			}
			candidates = append(candidates, eb.metadata[id])

			return true
		})
	}

	// Apply query filters and sorting
	filtered := backend.ApplyFilters(candidates, query)

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

	return &backend.QueryResult{
		Candidates: filtered[start:end],
		TotalCount: total,
		Paginating: end < total,
	}, nil
}
