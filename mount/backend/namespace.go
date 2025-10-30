package backend

type Namespace struct {
	Identifier string
}

// NamespacedKey combines namespace and key for non-SQL backends.
// Returns "namespace:key" format, or just "key" if namespace is empty.
func NamespacedKey(namespace, key string) string {
	if namespace == "" {
		return key
	}
	return namespace + ":" + key
}
