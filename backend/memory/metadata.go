package memory

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/mwantia/vfs/data"
)

func (mb *MemoryBackend) CreateMeta(ctx context.Context, meta *data.VirtualFileMetadata) error {
	// Allow empty key for mount root
	// if meta.Key == "" {
	// 	return data.ErrInvalid
	// }

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

func (mb *MemoryBackend) ReadAllMeta(ctx context.Context) ([]*data.VirtualFileMetadata, error) {
	metas := make([]*data.VirtualFileMetadata, 0)
	for _, meta := range mb.metadata {
		metas = append(metas, meta)
	}

	return metas, nil
}

func (mb *MemoryBackend) CreateAllMeta(ctx context.Context, metas []*data.VirtualFileMetadata) error {
	errs := data.Errors{}

	for _, meta := range metas {
		if err := mb.CreateMeta(ctx, meta); err != nil {
			errs.Add(err)
		}
	}

	return errs.Errors()
}
