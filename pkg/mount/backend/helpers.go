package backend

import (
	"fmt"
)

// NamedKey combines namespace and key for non-SQL backends.
// Returns `ns:key` format, or just `key` if namespace is empty.
func NamedKey(ns, key string) string {
	if ns == "" {
		return key
	}

	return fmt.Sprintf("%s:%s", ns, key)
}
