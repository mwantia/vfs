package namespace

import "github.com/mwantia/vfs/mount/backend"

type NamespaceBackendExtension interface {
	backend.Backend

	CreateNamespace(namespace string) (*backend.Namespace, error)

	GetNamespace(namespace string) (bool, *backend.Namespace)
}
