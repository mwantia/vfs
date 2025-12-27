package consul

import (
	"context"
	"io"
	"path"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

func (s *ConsulObjectStorageService) GetLifecycle() service.Lifecycle {
	return s.driver
}

func (s *ConsulObjectStorageService) CreateObject(ctx context.Context, ns, key string, mode data.FileMode) (*data.FileStat, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	named := s.buildNamedKey(ns, key)
	// Check if object already exists
	pair, _, err := s.driver.kv.Get(named, nil)
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
			parentConsulKey := s.buildNamedKey(ns, parentKey)

			// Check if parent is a file (files block directory creation)
			parent, _, err := s.driver.kv.Get(parentConsulKey, nil)
			if err == nil && parent != nil {
				// Parent exists as a key (file), can't create child
				return nil, data.ErrNotDirectory
			}

			// Parent doesn't exist as a key - check if it's a virtual directory
			prefix := parentConsulKey
			if prefix[len(prefix)-1] != '/' {
				prefix += "/"
			}
			keys, _, err := s.driver.kv.Keys(prefix, "", nil)
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

	now := time.Now()

	// Create the object
	if mode.IsDir() {
		// Create explicit directory key with trailing /
		// This allows empty directories to exist in Consul
		dirKey := named + "/"
		pair = &api.KVPair{
			Key:   dirKey,
			Value: []byte{}, // Empty value
			Flags: uint64(mode),
		}

		if _, err := s.driver.kv.Put(pair, nil); err != nil {
			return nil, err
		}
	} else {
		// Create file
		pair = &api.KVPair{
			Key:   named,
			Value: []byte{}, // Empty file
			Flags: uint64(mode),
		}

		if _, err := s.driver.kv.Put(pair, nil); err != nil {
			return nil, err
		}
	}

	return &data.FileStat{
		Key:        key,
		Mode:       mode,
		Size:       0,
		CreateTime: now,
		ModifyTime: now,
	}, nil
}

func (s *ConsulObjectStorageService) ReadObject(ctx context.Context, ns, key string, offset int64, buffer []byte) (int, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	named := s.buildNamedKey(ns, key)
	pair, _, err := s.driver.kv.Get(named, nil)
	if err != nil {
		return 0, err
	}
	if pair == nil {
		// Check if it's a virtual directory
		prefix := named + "/"
		keys, _, err := s.driver.kv.Keys(prefix, "", nil)
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
	toRead := min(int64(len(buffer)), available)

	// Copy data from buffer
	n := copy(buffer, pair.Value[offset:offset+toRead])
	return n, nil
}

func (s *ConsulObjectStorageService) WriteObject(ctx context.Context, ns, key string, offset int64, buffer []byte) (int, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	named := s.buildNamedKey(ns, key)
	pair, _, err := s.driver.kv.Get(named, nil)
	if err != nil {
		return 0, err
	}
	if pair == nil {
		// Check if it's a virtual directory
		prefix := named + "/"
		keys, _, err := s.driver.kv.Keys(prefix, "", nil)
		if err != nil {
			return 0, err
		}
		if len(keys) > 0 {
			return 0, data.ErrIsDirectory
		}
		return 0, data.ErrNotExist
	}

	// Key exists, so it's a file (directories are virtual and don't have keys)
	writeEnd := offset + int64(len(buffer))

	// Expand buffer if needed
	expBuffer := pair.Value
	if writeEnd > int64(len(expBuffer)) {
		newBuffer := make([]byte, writeEnd)
		copy(newBuffer, expBuffer)
		expBuffer = newBuffer
	}

	// Write the data
	copy(expBuffer[offset:], buffer)

	// Store back to Consul
	pair.Value = expBuffer
	if _, err := s.driver.kv.Put(pair, nil); err != nil {
		return 0, err
	}

	return len(buffer), nil
}

func (s *ConsulObjectStorageService) DeleteObject(ctx context.Context, ns, key string, force bool) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	named := s.buildNamedKey(ns, key)

	// Check for file key (without trailing /)
	pair, _, err := s.driver.kv.Get(named, nil)
	if err != nil {
		return err
	}

	// Check for explicit directory key (with trailing /)
	dirKey := named + "/"
	dirPair, _, err := s.driver.kv.Get(dirKey, nil)
	if err != nil {
		return err
	}

	// Check if it has children (virtual or explicit directory)
	keys, _, err := s.driver.kv.Keys(dirKey, "", nil)
	if err != nil {
		return err
	}

	hasChildren := len(keys) > 0
	isExplicitDir := dirPair != nil

	if hasChildren || isExplicitDir {
		// It's a directory (explicit or virtual)
		if !force {
			// Non-recursive delete requires force=true
			return data.ErrIsDirectory
		}

		// Delete all children recursively using DeleteTree
		if _, err := s.driver.kv.DeleteTree(dirKey, nil); err != nil {
			return err
		}

		// Delete the explicit directory key if it exists
		if isExplicitDir {
			if _, err := s.driver.kv.Delete(dirKey, nil); err != nil {
				return err
			}
		}

		return nil
	}

	// It's a file - delete the key
	if pair == nil {
		return data.ErrNotExist
	}

	if _, err := s.driver.kv.Delete(named, nil); err != nil {
		return err
	}

	return nil
}

func (s *ConsulObjectStorageService) ListObjects(ctx context.Context, ns, key string) ([]*data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	// Special case: if key is not root, check if it's actually a file
	if key != "" && key != "/" {
		named := s.buildNamedKey(ns, key)
		pair, _, err := s.driver.kv.Get(named, nil)

		if err == nil && pair != nil {
			// Key exists - it's a file
			mode := data.FileMode(pair.Flags)
			if mode == 0 {
				mode = 0644
			}

			return []*data.FileStat{{
				Key:        key,
				Mode:       mode,
				Size:       int64(len(pair.Value)),
				CreateTime: time.Unix(0, int64(pair.CreateIndex)),
				ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
			}}, nil
		}
		// Key doesn't exist - might be directory, check below
	}

	// Build Consul prefix for listing children
	namedPrefix := s.buildNamedKey(ns, key)
	if namedPrefix != "" {
		namedPrefix += "/"
	}

	// Get direct children only (separator "/" tells Consul to only return one level)
	childrens, _, err := s.driver.kv.Keys(namedPrefix, "/", nil)
	if err != nil {
		return nil, err
	}

	// If no children and key doesn't exist, directory doesn't exist
	if len(childrens) == 0 && key != "" && key != "/" {
		named := s.buildNamedKey(ns, key)

		// Check for file key
		pair, _, _ := s.driver.kv.Get(named, nil)
		if pair != nil {
			// It's a file, not a directory - this is fine, return empty list
			return nil, data.ErrNotExist
		}

		// Check for explicit directory key
		dirPair, _, _ := s.driver.kv.Get(named+"/", nil)
		if dirPair == nil {
			// Neither file nor directory exists
			return nil, data.ErrNotExist
		}
	}

	// Build result list
	result := make([]*data.FileStat, 0, len(childrens))

	for _, child := range childrens {
		// Skip the directory's own key (exact match of the prefix)
		// For example, when listing "empty/", skip "empty/" itself
		if child == namedPrefix {
			continue
		}

		// Check if this is a virtual directory (Consul adds trailing / for prefixes)
		hasTrailingSlash := len(child) > 0 && child[len(child)-1] == '/'

		// Extract basename (last segment after /)
		var basename string
		if hasTrailingSlash {
			// "dns/policies/" -> "dns/policies" -> "policies"
			trimmed := child[:len(child)-1]
			idx := len(trimmed) - 1
			for idx >= 0 && trimmed[idx] != '/' {
				idx--
			}
			basename = trimmed[idx+1:]
		} else {
			// "dns/config" -> "config"
			idx := len(child) - 1
			for idx >= 0 && child[idx] != '/' {
				idx--
			}
			basename = child[idx+1:]
		}

		// The key should just be the basename (relative to parent directory)
		relKey := basename

		if hasTrailingSlash {
			// Directory (explicit or virtual) - fetch the key to get metadata
			pair, _, err := s.driver.kv.Get(child, nil)
			if err != nil || pair == nil {
				// Virtual directory - no explicit key
				result = append(result, &data.FileStat{
					Key:        relKey,
					Mode:       0755 | data.ModeDir,
					Size:       0,
					CreateTime: time.Now(),
					ModifyTime: time.Now(),
				})
			} else {
				// Explicit directory key - use stored metadata
				mode := data.FileMode(pair.Flags)
				if mode == 0 {
					mode = 0755 | data.ModeDir
				}

				result = append(result, &data.FileStat{
					Key:        relKey,
					Mode:       mode,
					Size:       0,
					CreateTime: time.Unix(0, int64(pair.CreateIndex)),
					ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
				})
			}
		} else {
			// File key
			pair, _, err := s.driver.kv.Get(child, nil)
			if err != nil || pair == nil {
				continue
			}

			mode := data.FileMode(pair.Flags)
			if mode == 0 {
				mode = 0644
			}

			result = append(result, &data.FileStat{
				Key:        relKey,
				Mode:       mode,
				Size:       int64(len(pair.Value)),
				CreateTime: time.Unix(0, int64(pair.CreateIndex)),
				ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
			})
		}
	}

	return result, nil
}

func (s *ConsulObjectStorageService) HeadObject(ctx context.Context, ns, key string) (*data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	named := s.buildNamedKey(ns, key)

	// Check for file key (without trailing /)
	pair, _, err := s.driver.kv.Get(named, nil)
	if err == nil && pair != nil {
		mode := data.FileMode(pair.Flags)
		if mode == 0 {
			mode = 0644 // Default file mode
		}

		return &data.FileStat{
			Key:        key,
			Mode:       mode,
			Size:       int64(len(pair.Value)),
			CreateTime: time.Unix(0, int64(pair.CreateIndex)),
			ModifyTime: time.Unix(0, int64(pair.ModifyIndex)),
		}, nil
	}

	// Check for explicit directory key (with trailing /)
	dirKey := named + "/"
	dirPair, _, err := s.driver.kv.Get(dirKey, nil)
	if err == nil && dirPair != nil {
		mode := data.FileMode(dirPair.Flags)
		if mode == 0 {
			mode = 0755 | data.ModeDir // Default directory mode
		}

		return &data.FileStat{
			Key:        key,
			Mode:       mode,
			Size:       0,
			CreateTime: time.Unix(0, int64(dirPair.CreateIndex)),
			ModifyTime: time.Unix(0, int64(dirPair.ModifyIndex)),
		}, nil
	}

	// Check if it's a virtual directory (has children)
	keys, _, err := s.driver.kv.Keys(dirKey, "", nil)
	if err != nil {
		return nil, err
	}

	if len(keys) > 0 {
		// It's a virtual directory (has children but no explicit key)
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

func (s *ConsulObjectStorageService) TruncateObject(ctx context.Context, ns, key string, size int64) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	named := s.buildNamedKey(ns, key)
	pair, _, err := s.driver.kv.Get(named, nil)
	if err != nil {
		return err
	}
	if pair == nil {
		// Check if it's a virtual directory
		prefix := named + "/"
		keys, _, err := s.driver.kv.Keys(prefix, "", nil)
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
	if _, err := s.driver.kv.Put(pair, nil); err != nil {
		return err
	}

	return nil
}

// buildKey constructs the full Consul KV key from the object key
func (s *ConsulObjectStorageService) buildNamedKey(ns, key string) string {
	// Remove leading / from key if present
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// Handle "/" prefix specially - it means no prefix, just use the key
	if s.driver.cfg.Prefix == "/" {
		return service.NamedKey(ns, key, "/")
	}

	// For other prefixes, ensure they end with /
	prefix := s.driver.cfg.Prefix
	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	return service.NamedKey(ns, prefix+key, "/")
}
