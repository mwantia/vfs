package vfs

import (
	"path"
	"strings"
)

// cleanPath normalizes a path for use in the VFS.
// It removes leading/trailing slashes, resolves . and ..,
// and removes duplicate slashes.
func cleanPath(p string) string {
	// Use path.Clean to normalize
	cleaned := path.Clean(p)

	// Remove leading slash to make it relative
	cleaned = strings.TrimPrefix(cleaned, "/")

	// Handle root path
	if cleaned == "." {
		return ""
	}

	return cleaned
}

// hasPrefix checks if path has the given prefix.
// Both paths should be cleaned before calling.
func hasPrefix(path, prefix string) bool {
	// Root matches everything
	if prefix == "" {
		return true
	}

	// Exact match
	if path == prefix {
		return true
	}

	// Check if path starts with prefix followed by /
	return strings.HasPrefix(path, prefix+"/")
}

// trimPrefix removes the prefix from path.
// Returns the relative path after the prefix.
func trimPrefix(path, prefix string) string {
	if prefix == "" {
		return path
	}

	if path == prefix {
		return ""
	}

	// Remove prefix and leading slash
	rel := strings.TrimPrefix(path, prefix)
	rel = strings.TrimPrefix(rel, "/")

	return rel
}
