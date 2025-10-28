package memory

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mwantia/vfs/backend"
	"github.com/mwantia/vfs/data"
)

func (mb *MemoryBackend) CreateMeta(ctx context.Context, meta *data.VirtualFileMetadata) error {
	// Populate unique ID if not already defined
	if meta.ID == "" {
		meta.ID = uuid.Must(uuid.NewV7()).String()
	}

	// Update CreateTime if
	if meta.CreateTime.IsZero() {
		meta.CreateTime = time.Now()
	}

	mb.keys.Set(meta.Key, meta.ID)
	mb.metadata[meta.ID] = meta

	// Update directory index for fast lookups
	// Extract parent directory from key
	if idx := strings.LastIndex(meta.Key, "/"); idx >= 0 {
		// Parent directory path (everything before last /)
		parentDir := meta.Key[:idx+1]
		// Add this key to parent's children list
		if mb.directories[parentDir] == nil {
			mb.directories[parentDir] = make([]string, 0)
		}
		mb.directories[parentDir] = append(mb.directories[parentDir], meta.Key)
	}

	return nil
}

func (mb *MemoryBackend) ReadMeta(ctx context.Context, key string) (*data.VirtualFileMetadata, error) {
	id, exists := mb.keys.Get(key)
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

func (mb *MemoryBackend) UpdateMeta(ctx context.Context, key string, update *data.VirtualFileMetadataUpdate) error {
	id, exists := mb.keys.Get(key)
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

func (mb *MemoryBackend) DeleteMeta(ctx context.Context, key string) error {
	id, exists := mb.keys.Get(key)
	if !exists {
		return data.ErrNotExist
	}

	if _, ok := mb.keys.Delete(key); ok {
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
			if children, ok := mb.directories[parentDir]; ok {
				// Find and remove this key from children list
				for i, child := range children {
					if child == key {
						// Remove by swapping with last element and truncating
						mb.directories[parentDir] = append(children[:i], children[i+1:]...)
						break
					}
				}
				// Clean up empty directory entries
				if len(mb.directories[parentDir]) == 0 {
					delete(mb.directories, parentDir)
				}
			}
		}
	}

	return nil
}

func (mb *MemoryBackend) ExistsMeta(ctx context.Context, key string) (bool, error) {
	id, exists := mb.keys.Get(key)
	if !exists {
		return false, data.ErrNotExist
	}

	_, exists = mb.metadata[id]
	if !exists {
		return false, data.ErrNotExist
	}

	return true, nil
}

func (mb *MemoryBackend) QueryMeta(ctx context.Context, query *backend.MetadataQuery) (*backend.MetadataQueryResult, error) {
	var candidates []*data.VirtualFileMetadata

	if query.Delimiter == "/" {
		// Delimiter mode: return only direct children
		if query.Prefix != "" {
			// Non-empty prefix: use pre-computed directory index
			if children, ok := mb.directories[query.Prefix]; ok {
				for _, key := range children {
					id, _ := mb.keys.Get(key)
					candidates = append(candidates, mb.metadata[id])
				}
			}
		} else {
			// Empty prefix (root): return only top-level entries (no "/" in key)
			for _, meta := range mb.metadata {
				if !strings.Contains(meta.Key, "/") {
					candidates = append(candidates, meta)
				}
			}
		}
	} else {
		// No delimiter: return all entries matching prefix (recursive)
		for _, meta := range mb.metadata {
			if query.Prefix == "" || strings.HasPrefix(meta.Key, query.Prefix) {
				candidates = append(candidates, meta)
			}
		}
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
