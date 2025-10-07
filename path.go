package vfs

import (
	"strings"
)

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
	return strings.HasPrefix(path, prefix)
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
