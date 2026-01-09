package mount

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/errors"
)

func (m *Mount) resolveMountPoint(path string) (MountPoint, string) {
	path = strings.TrimSpace(path)
	if path != "" && path != "/" {
		m.mu.RLock()
		defer m.mu.RUnlock()

		var bestMatch MountPoint
		var bestPrefix string

		for mountPath, mount := range m.fstab {
			// Normalize paths for comparison
			normalizedMountPath := strings.Trim(mountPath, "/")
			normalizedInputPath := strings.Trim(path, "/")
			// Check if mountpath is a matching prefix
			if normalizedInputPath == normalizedMountPath || strings.HasPrefix(normalizedInputPath, normalizedMountPath+"/") {
				// Keep the longest match
				if len(normalizedMountPath) > len(bestPrefix) {
					bestMatch = mount
					bestPrefix = normalizedMountPath
				}
			}
		}
		// Found a matching submount to traverse to
		if bestPrefix != "" {
			prefix := strings.Trim(bestPrefix, "/")
			newPath := path
			// Remove the prefix and any following slash
			if prefix != "" {
				if after, ok := strings.CutPrefix(newPath, prefix+"/"); ok {
					newPath = after
				} else if newPath == prefix {
					newPath = ""
				}
			}

			return bestMatch, newPath
		}
	}
	// No submount found, handle locally
	return m, path
}

// findDirectChildMounts returns mount entries that are direct children of the given path.
// For example, if path is "a" and fstab contains ["a/b/c", "a/d", "x/y"]:
//   - Returns: ["b", "d"] (direct children only)
//   - Excludes: "b/c" (nested under b), "x/y" (different parent)
//
// Thread-safety: Caller must hold m.mu (RLock or Lock).
func (m *Mount) findDirectChildMounts(path string) []string {
	// Normalize path for comparison
	normalizedPath := strings.Trim(path, "/")

	// Root path special case
	var searchPrefix string
	if normalizedPath == "" {
		searchPrefix = "" // Root - no prefix needed
	} else {
		searchPrefix = normalizedPath + "/"
	}

	childNames := make(map[string]bool)

	for mountPath := range m.fstab {
		normalizedMount := strings.Trim(mountPath, "/")

		// Root path: show top-level mounts (no "/" in path)
		if normalizedPath == "" {
			// Split mount path and take first segment
			parts := strings.SplitN(normalizedMount, "/", 2)
			if parts[0] != "" {
				childNames[parts[0]] = true
			}
			continue
		}

		// Non-root: check prefix match
		if !strings.HasPrefix(normalizedMount, searchPrefix) {
			continue // Not under this path
		}

		// Get relative path after prefix
		relativePath := strings.TrimPrefix(normalizedMount, searchPrefix)

		// Only direct children (no nested slashes)
		parts := strings.SplitN(relativePath, "/", 2)
		if parts[0] != "" {
			childNames[parts[0]] = true
		}
	}

	// Convert map to sorted slice for deterministic output
	result := make([]string, 0, len(childNames))
	for name := range childNames {
		result = append(result, name)
	}

	// Sort alphabetically
	sort.Strings(result)

	return result
}

func (m *Mount) matchAnyMountPoints(path string) bool {
	if path != "" {
		for k := range m.fstab {
			if strings.HasPrefix(path, k) {
				return true
			}
		}
	}

	return false
}

// createMountMetadata generates synthetic metadata for a mount entry.
// The metadata is virtual (not persisted) and uses mount creation time.
func (m *Mount) createMountMetadata(key string) data.Metadata {
	now := m.createTime // Use mount creation time for consistency
	id := uuid.Must(uuid.NewV7()).String()

	return data.Metadata{
		ID:          id,
		Key:         key,
		Mode:        data.ModeMount | data.ModeDir | 0555, // Mount + Directory + r-xr-xr-x
		Size:        0,                                    // Directories have 0 size
		AccessTime:  now,
		ModifyTime:  now,
		CreateTime:  now,
		ContentType: "", // Directories have no content type
		Attributes: map[string]string{
			"mount.virtual": "true", // Mark as virtual mount entry
		},
	}
}

// injectMountEntries adds mount entries to directory listing and handles path conflicts.
// Mount entries shadow any real directories with the same name (mount takes precedence).
//
// Thread-safety: Caller must hold m.mu (at least RLock).
func (m *Mount) injectMountEntries(dirPath string, entries []data.Metadata) []data.Metadata {
	// Find direct child mounts for this path
	childMounts := m.findDirectChildMounts(dirPath)

	if len(childMounts) == 0 {
		return entries // No mounts to inject
	}

	// Create mount metadata entries
	mountEntries := make(map[string]data.Metadata, len(childMounts))
	for _, mountName := range childMounts {
		// Build full key for metadata
		var fullKey string
		if dirPath == "" || dirPath == "/" {
			fullKey = mountName
		} else {
			fullKey = dirPath + "/" + mountName
		}

		mountEntries[mountName] = m.createMountMetadata(fullKey)
	}

	// Deduplicate: remove real entries that conflict with mounts (mount shadows)
	filtered := make([]data.Metadata, 0, len(entries))
	for _, entry := range entries {
		// Extract base name from entry key
		baseName := entry.Key
		if idx := strings.LastIndex(entry.Key, "/"); idx >= 0 {
			baseName = entry.Key[idx+1:]
		}

		// Skip if mount exists with same name (mount shadows directory)
		if _, isMountPoint := mountEntries[baseName]; !isMountPoint {
			filtered = append(filtered, entry)
		}
	}

	// Combine filtered entries + mount entries
	result := make([]data.Metadata, 0, len(filtered)+len(mountEntries))
	result = append(result, filtered...)
	for _, mountMeta := range mountEntries {
		result = append(result, mountMeta)
	}

	// Sort by key for deterministic output
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

func (m *Mount) addObjectStoragePathPrefix(path string) string {
	if m.Options.PathPrefix == "" {
		return path
	}

	var prefixedPath string
	if strings.HasSuffix(m.Options.PathPrefix, "/") {
		prefixedPath = m.Options.PathPrefix + path
	} else {
		prefixedPath = m.Options.PathPrefix + "/" + path
	}
	after, _ := strings.CutSuffix(prefixedPath, "/")
	return after
}

func (m *Mount) removeObjectStoragePathPrefix(prefixedPath string) string {
	if m.Options.PathPrefix == "" {
		return prefixedPath
	}

	pathPrefix := m.Options.PathPrefix
	if !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix = pathPrefix + "/"
	}

	return strings.TrimPrefix(prefixedPath, pathPrefix)
}

// validatePathLength checks if the path exceeds the maximum path length constraint
func (m *Mount) validatePathLength(path string) error {
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	maxLength := caps.ObjectStorage.Constraints.MaxPathLength
	if maxLength > 0 && len(path) > maxLength {
		return fmt.Errorf("%w: path length %d exceeds maximum %d", errors.ErrPathTooLong, len(path), maxLength)
	}
	return nil
}

// validatePathDepth checks if the path exceeds the maximum depth constraint
func (m *Mount) validatePathDepth(path string) error {
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	maxDepth := caps.ObjectStorage.Constraints.MaxPathDepth
	if maxDepth > 0 {
		// Count directory separators to determine depth
		// Empty path = depth 0, "a" = depth 1, "a/b" = depth 2, etc.
		depth := 0
		if path != "" {
			depth = 1
			for _, char := range path {
				if char == '/' {
					depth++
				}
			}
		}
		if depth > maxDepth {
			return fmt.Errorf("%w: path depth %d exceeds maximum %d", errors.ErrPathTooDeep, depth, maxDepth)
		}
	}
	return nil
}

// validateObjectSize checks if the object size violates size constraints
func (m *Mount) validateObjectSize(size int64) error {
	caps := m.ObjectStorage.GetLifecycle().GetCapabilities()
	constraints := caps.ObjectStorage.Constraints

	if constraints.MinObjectSize > 0 && size < constraints.MinObjectSize {
		return fmt.Errorf("%w: object size %d is below minimum %d", errors.ErrObjectTooSmall, size, constraints.MinObjectSize)
	}

	if constraints.MaxObjectSize > 0 && size > constraints.MaxObjectSize {
		return fmt.Errorf("%w: object size %d exceeds maximum %d", errors.ErrObjectTooLarge, size, constraints.MaxObjectSize)
	}

	return nil
}
