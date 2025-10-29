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

// Directory marker - directories are stored with this special value
const dirMarker = "__DIR__"

// CreateObject creates a new object (file or directory)
func (cb *ConsulBackend) CreateObject(ctx context.Context, key string, mode data.VirtualFileMode) (*data.VirtualFileStat, error) {
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
			parent, _, err := cb.kv.Get(parentConsulKey, nil)

			// If parent exists as a key
			if err == nil && parent != nil {
				// Check if parent is a directory
				if string(parent.Value) != dirMarker {
					return nil, data.ErrNotDirectory
				}
			} else {
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
	}

	// Create the object
	var value []byte
	if mode.IsDir() {
		value = []byte(dirMarker)
	} else {
		value = []byte{} // Empty file
	}

	pair = &api.KVPair{
		Key:   consulKey,
		Value: value,
	}

	if _, err := cb.kv.Put(pair, nil); err != nil {
		return nil, err
	}

	now := time.Now()
	return &data.VirtualFileStat{
		Key:        key,
		Mode:       mode,
		Size:       0,
		CreateTime: now,
		ModifyTime: now,
	}, nil
}

// ReadObject reads data from an object at a given offset
func (cb *ConsulBackend) ReadObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return 0, err
	}
	if pair == nil {
		return 0, data.ErrNotExist
	}

	// Check if it's a directory
	if string(pair.Value) == dirMarker {
		return 0, data.ErrIsDirectory
	}

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
func (cb *ConsulBackend) WriteObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return 0, err
	}
	if pair == nil {
		return 0, data.ErrNotExist
	}

	// Check if it's a directory
	if string(pair.Value) == dirMarker {
		return 0, data.ErrIsDirectory
	}

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
func (cb *ConsulBackend) DeleteObject(ctx context.Context, key string, force bool) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return err
	}
	if pair == nil {
		return data.ErrNotExist
	}

	isDir := string(pair.Value) == dirMarker

	if isDir {
		// For directories, check if we need to delete recursively
		if !force {
			// Check if directory has children
			prefix := consulKey
			if prefix[len(prefix)-1] != '/' {
				prefix += "/"
			}
			keys, _, err := cb.kv.Keys(prefix, "", nil)
			if err != nil {
				return err
			}
			if len(keys) > 0 {
				return data.ErrIsDirectory
			}
		} else {
			// Delete all children recursively using DeleteTree
			prefix := consulKey
			if prefix[len(prefix)-1] != '/' {
				prefix += "/"
			}
			if _, err := cb.kv.DeleteTree(prefix, nil); err != nil {
				return err
			}
		}
	}

	// Delete the object itself
	if _, err := cb.kv.Delete(consulKey, nil); err != nil {
		return err
	}

	return nil
}

// ListObjects lists all objects under a given key (directory)
func (cb *ConsulBackend) ListObjects(ctx context.Context, key string) ([]*data.VirtualFileStat, error) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	// Special case: if key is not root, check if it's actually a file
	if key != "" && key != "/" {
		consulKey := cb.buildKey(key)
		pair, _, err := cb.kv.Get(consulKey, nil)

		if err == nil && pair != nil {
			// Key exists - check if it's a file
			if string(pair.Value) != dirMarker {
				// It's a file, return just this file
				return []*data.VirtualFileStat{{
					Key:        key,
					Mode:       0644,
					Size:       int64(len(pair.Value)),
					CreateTime: time.Unix(0, int64(pair.CreateIndex)),
					ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
				}}, nil
			}
			// It's a directory marker - continue to list children
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
	result := make([]*data.VirtualFileStat, 0, len(consulKeys))

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
			result = append(result, &data.VirtualFileStat{
				Key:        relKey,
				Mode:       0755 | data.ModeDir,
				Size:       0,
				CreateTime: time.Now(),
				ModifyTime: time.Now(),
			})
		} else {
			// Real key - fetch metadata
			pair, _, err := cb.kv.Get(consulKey, nil)
			if err != nil || pair == nil {
				continue
			}

			var mode data.VirtualFileMode
			var size int64

			if string(pair.Value) == dirMarker {
				mode = 0755 | data.ModeDir
				size = 0
			} else {
				mode = 0644
				size = int64(len(pair.Value))
			}

			result = append(result, &data.VirtualFileStat{
				Key:        relKey,
				Mode:       mode,
				Size:       size,
				CreateTime: time.Unix(0, int64(pair.CreateIndex)),
				ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
			})
		}
	}

	return result, nil
}

// HeadObject returns metadata about an object
func (cb *ConsulBackend) HeadObject(ctx context.Context, key string) (*data.VirtualFileStat, error) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)

	// If key exists, return its metadata
	if err == nil && pair != nil {
		var mode data.VirtualFileMode
		var size int64

		if string(pair.Value) == dirMarker {
			mode = 0755 | data.ModeDir
			size = 0
		} else {
			mode = 0644
			size = int64(len(pair.Value))
		}

		return &data.VirtualFileStat{
			Key:        key,
			Mode:       mode,
			Size:       size,
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
		return &data.VirtualFileStat{
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
func (cb *ConsulBackend) TruncateObject(ctx context.Context, key string, size int64) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	consulKey := cb.buildKey(key)
	pair, _, err := cb.kv.Get(consulKey, nil)
	if err != nil {
		return err
	}
	if pair == nil {
		return data.ErrNotExist
	}

	// Check if it's a directory
	if string(pair.Value) == dirMarker {
		return data.ErrIsDirectory
	}

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
