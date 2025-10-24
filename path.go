package vfs

import (
	"fmt"
	"strings"

	"github.com/mwantia/vfs/data"
)

// ToAbsolutePath ensures the path always starts with a leading slash.
func ToAbsolutePath(path string) (string, error) {
	if len(path) == 0 {
		return "", data.ErrInvalidPath
	}

	if !strings.HasPrefix(path, "/") {
		path = fmt.Sprintf("/%s", path)
	}

	return path, nil
}

// ToRelativePath removes the prefix from path.
// Returns the relative path after the prefix.
// It additionally removes any leading slashes.
func ToRelativePath(path, prefix string) string {
	if prefix == "" {
		return path
	}

	if path == prefix {
		return ""
	}

	relPath := strings.TrimPrefix(path, prefix)
	return strings.TrimPrefix(relPath, "/")
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
	return strings.HasPrefix(path, prefix)
}
