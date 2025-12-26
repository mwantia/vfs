package stub

import (
	"github.com/mwantia/vfs/context"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

func (s *StubObjectStorageDriver) GetLifecycle() service.Lifecycle {
	return s
}

func (*StubObjectStorageDriver) CreateObject(traversal context.TraversalContext, ns, key string, mode data.FileMode) (*data.FileStat, error) {
	return nil, nil
}

func (*StubObjectStorageDriver) ReadObject(traversal context.TraversalContext, ns, key string, offset int64, data []byte) (int, error) {
	return 0, nil
}

func (*StubObjectStorageDriver) WriteObject(traversal context.TraversalContext, ns, key string, offset int64, data []byte) (int, error) {
	return 0, nil
}

func (*StubObjectStorageDriver) DeleteObject(traversal context.TraversalContext, ns, key string, force bool) error {
	return nil
}

func (*StubObjectStorageDriver) ListObjects(traversal context.TraversalContext, ns, key string) ([]*data.FileStat, error) {
	return nil, nil
}

func (*StubObjectStorageDriver) HeadObject(traversal context.TraversalContext, ns, key string) (*data.FileStat, error) {
	return nil, nil
}

func (*StubObjectStorageDriver) TruncateObject(traversal context.TraversalContext, ns, key string, size int64) error {
	return nil
}
