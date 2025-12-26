package service

import (
	"fmt"
)

// NamedKey combines namespace and key for non-SQL backends.
// Returns `ns:key` format, or just `key` if namespace is empty.
func NamedKey(ns, key, sep string) string {
	if ns == "" {
		return key
	}
	if sep == "" {
		sep = ":"
	}

	return fmt.Sprintf("%s%s%s", ns, sep, key)
}
