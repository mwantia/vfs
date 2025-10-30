package consul

import (
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/mwantia/vfs/data"
)

// CreateObject creates a new object (file or directory)
func (cb *ConsulBackend) CreateObject(ctx context.Context, namespace, key string, mode data.FileMode) (*data.FileStat, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	consulKey := cb.buildKey(key)

	// Check if object already exists
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return nil, err
	}
	if pair != nil {
		return nil, data.ErrExist
	}

	// Verify parent directory exists (except for root)
	if key != "" && key != "/" {
		parentKey := path.Dir(key)
		if parentKey != "." && parentKey != "" && parentKey != "/" {
			parentConsulKey := cb.buildKey(parentKey)

			// Check if parent is a file (files block directory creation)
			parent, _, err := cb.kv.Get(parentConsulKey, nil)
			if err == nil && parent != nil {
				// Parent exists as a key (file), can't create child
				return nil, data.ErrNotDirectory
			}

			// Parent doesn't exist as a key - check if it's a virtual directory
			prefix := parentConsulKey
			if prefix[len(prefix)-1] != '/' {
				prefix += "/"
			}
			keys, _, err := cb.kv.Keys(prefix, "", nil)
			if err != nil {
				return nil, err
			}
			if len(keys) == 0 {
				// No children, so parent doesn't exist
				return nil, data.ErrNotExist
			}
			// Parent is a virtual directory - allow creation
		}
	}

	// Create the object
	// For directories: don't create a key, they're virtual (exist as prefixes only)
	// For files: create a key with empty or actual data
	if mode.IsDir() {
		// Directories are virtual in Consul - don't create a key
		// Just return success
		now := time.Now()
		return &data.FileStat{
			Key:        key,
			Mode:       mode,
			Size:       0,
			CreateTime: now,
			ModifyTime: now,
		}, nil
	}

	// Create file
	pair = &api.KVPair{
		Key:   consulKey,
		Value: []byte{}, // Empty file
	}

	if _, err := cb.kv.Put(pair, nil); err != nil {
		return nil, err
	}

	now := time.Now()
	return &data.FileStat{
		Key:        key,
		Mode:       mode,
		Size:       0,
		CreateTime: now,
		ModifyTime: now,
	}, nil
}

// ReadObject reads data from an object at a given offset
func (cb *ConsulBackend) ReadObject(ctx context.Context, namespace, key string, offset int64, dat []byte) (int, error) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return 0, err
	}
	if pair == nil {
		// Check if it's a virtual directory
		prefix := consulKey + "/"
		keys, _, err := cb.kv.Keys(prefix, "", nil)
		if err != nil {
			return 0, err
		}
		if len(keys) > 0 {
			return 0, data.ErrIsDirectory
		}
		return 0, data.ErrNotExist
	}

	// Key exists, so it's a file (directories are virtual and don't have keys)

	size := int64(len(pair.Value))
	if offset >= size {
		return 0, io.EOF
	}

	// Calculate how many bytes we can actually read
	available := size - offset
	toRead := min(int64(len(dat)), available)

	// Copy data from buffer
	n := copy(dat, pair.Value[offset:offset+toRead])
	return n, nil
}

// WriteObject writes data to an object at a given offset
func (cb *ConsulBackend) WriteObject(ctx context.Context, namespace, key string, offset int64, dat []byte) (int, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return 0, err
	}
	if pair == nil {
		// Check if it's a virtual directory
		prefix := consulKey + "/"
		keys, _, err := cb.kv.Keys(prefix, "", nil)
		if err != nil {
			return 0, err
		}
		if len(keys) > 0 {
			return 0, data.ErrIsDirectory
		}
		return 0, data.ErrNotExist
	}

	// Key exists, so it's a file (directories are virtual and don't have keys)

	writeEnd := offset + int64(len(dat))

	// Check size constraint from capabilities
	capabilities := cb.GetCapabilities()
	if capabilities.MaxObjectSize > 0 && writeEnd > capabilities.MaxObjectSize {
		return 0, fmt.Errorf("write would exceed max object size of %d bytes (Consul KV limit: 512KB)", capabilities.MaxObjectSize)
	}

	// Expand buffer if needed
	buffer := pair.Value
	if writeEnd > int64(len(buffer)) {
		newBuffer := make([]byte, writeEnd)
		copy(newBuffer, buffer)
		buffer = newBuffer
	}

	// Write the data
	copy(buffer[offset:], dat)

	// Store back to Consul
	pair.Value = buffer
	if _, err := cb.kv.Put(pair, nil); err != nil {
		return 0, err
	}

	return len(dat), nil
}

// DeleteObject deletes an object (file or directory)
func (cb *ConsulBackend) DeleteObject(ctx context.Context, namespace, key string, force bool) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return err
	}

	// Check if it's a virtual directory
	prefix := consulKey + "/"
	keys, _, err := cb.kv.Keys(prefix, "", nil)
	if err != nil {
		return err
	}

	isDir := len(keys) > 0

	if isDir {
		// It's a directory (virtual)
		if !force {
			// Non-recursive delete on non-empty directory should fail
			return data.ErrIsDirectory
		}
		// Delete all children recursively using DeleteTree
		if _, err := cb.kv.DeleteTree(prefix, nil); err != nil {
			return err
		}
		// If there was an explicit key (shouldn't happen for virtual dirs), delete it too
		if pair != nil {
			if _, err := cb.kv.Delete(consulKey, nil); err != nil {
				return err
			}
		}
		return nil
	}

	// It's a file - delete the key
	if pair == nil {
		return data.ErrNotExist
	}

	if _, err := cb.kv.Delete(consulKey, nil); err != nil {
		return err
	}

	return nil
}

// ListObjects lists all objects under a given key (directory)
func (cb *ConsulBackend) ListObjects(ctx context.Context, namespace, key string) ([]*data.FileStat, error) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	// Special case: if key is not root, check if it's actually a file
	if key != "" && key != "/" {
		consulKey := cb.buildKey(key)
		pair, _, err := cb.kv.Get(consulKey, nil)

		if err == nil && pair != nil {
			// Key exists - it's a file (directories are virtual and don't have keys)
			return []*data.FileStat{{
				Key:        key,
				Mode:       0644,
				Size:       int64(len(pair.Value)),
				CreateTime: time.Unix(0, int64(pair.CreateIndex)),
				ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
			}}, nil
		}
		// Key doesn't exist - might be virtual directory, check below
	}

	// Build Consul prefix for listing children
	consulPrefix := cb.buildKey(key)
	if consulPrefix != "" {
		consulPrefix += "/"
	}

	// Get direct children only (separator "/" tells Consul to only return one level)
	consulKeys, _, err := cb.kv.Keys(consulPrefix, "/", nil)
	if err != nil {
		return nil, err
	}

	// If no children and key doesn't exist, directory doesn't exist
	if len(consulKeys) == 0 && key != "" && key != "/" {
		pair, _, _ := cb.kv.Get(cb.buildKey(key), nil)
		if pair == nil {
			return nil, data.ErrNotExist
		}
	}

	// Build result list
	result := make([]*data.FileStat, 0, len(consulKeys))

	for _, consulKey := range consulKeys {
		// Check if this is a virtual directory (Consul adds trailing / for prefixes)
		hasTrailingSlash := len(consulKey) > 0 && consulKey[len(consulKey)-1] == '/'

		// Extract basename (last segment after /)
		var basename string
		if hasTrailingSlash {
			// "dns/policies/" -> "dns/policies" -> "policies"
			trimmed := consulKey[:len(consulKey)-1]
			idx := len(trimmed) - 1
			for idx >= 0 && trimmed[idx] != '/' {
				idx--
			}
			basename = trimmed[idx+1:]
		} else {
			// "dns/config" -> "config"
			idx := len(consulKey) - 1
			for idx >= 0 && consulKey[idx] != '/' {
				idx--
			}
			basename = consulKey[idx+1:]
		}

		// The key should just be the basename (relative to parent directory)
		relKey := basename

		if hasTrailingSlash {
			// Virtual directory - no actual key, just a prefix
			result = append(result, &data.FileStat{
				Key:        relKey,
				Mode:       0755 | data.ModeDir,
				Size:       0,
				CreateTime: time.Now(),
				ModifyTime: time.Now(),
			})
		} else {
			// Real key - it's a file (directories are virtual and don't have keys)
			pair, _, err := cb.kv.Get(consulKey, nil)
			if err != nil || pair == nil {
				continue
			}

			result = append(result, &data.FileStat{
				Key:        relKey,
				Mode:       0644,
				Size:       int64(len(pair.Value)),
				CreateTime: time.Unix(0, int64(pair.CreateIndex)),
				ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
			})
		}
	}

	return result, nil
}

// HeadObject returns metadata about an object
func (cb *ConsulBackend) HeadObject(ctx context.Context, namespace, key string) (*data.FileStat, error) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)

	// If key exists, it's a file (directories are virtual and don't have keys)
	if err == nil && pair != nil {
		return &data.FileStat{
			Key:        key,
			Mode:       0644,
			Size:       int64(len(pair.Value)),
			CreateTime: time.Unix(0, int64(pair.CreateIndex)),
			ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
		}, nil
	}

	// Key doesn't exist - check if it's a virtual directory
	prefix := consulKey
	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	keys, _, err := cb.kv.Keys(prefix, "", nil)
	if err != nil {
		return nil, err
	}
	if len(keys) > 0 {
		// It's a virtual directory (has children but no actual key)
		return &data.FileStat{
			Key:        key,
			Mode:       0755 | data.ModeDir,
			Size:       0,
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
		}, nil
	}

	// Doesn't exist at all
	return nil, data.ErrNotExist
}

// TruncateObject truncates an object to a specific size
func (cb *ConsulBackend) TruncateObject(ctx context.Context, namespace, key string, size int64) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return err
	}
	if pair == nil {
		// Check if it's a virtual directory
		prefix := consulKey + "/"
		keys, _, err := cb.kv.Keys(prefix, "", nil)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			return data.ErrIsDirectory
		}
		return data.ErrNotExist
	}

	// Key exists, so it's a file (directories are virtual and don't have keys)

	currentSize := int64(len(pair.Value))
	if size == currentSize {
		return nil // No changes needed
	}

	// Check size constraint from capabilities
	capabilities := cb.GetCapabilities()
	if capabilities.MaxObjectSize > 0 && size > capabilities.MaxObjectSize {
		return fmt.Errorf("truncate size %d exceeds max object size of %d bytes (Consul KV limit: 512KB)", size, capabilities.MaxObjectSize)
	}

	if size < currentSize {
		// Shrink file
		pair.Value = pair.Value[:size]
	} else {
		// Expand file with zeros
		newData := make([]byte, size)
		copy(newData, pair.Value)
		pair.Value = newData
	}

	// Store back to Consul
	if _, err := cb.kv.Put(pair, nil); err != nil {
		return err
	}

	return nil
}
