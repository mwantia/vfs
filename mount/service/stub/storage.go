package stub

import (
	"context"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

func (s *StubObjectStorageDriver) GetLifecycle() service.Lifecycle {
	return s
}

func (*StubObjectStorageDriver) CreateObject(ctx context.Context, ns, key string, mode data.FileMode) (*data.FileStat, error) {
	return nil, nil
}

func (*StubObjectStorageDriver) ReadObject(ctx context.Context, ns, key string, offset int64, data []byte) (int, error) {
	return 0, nil
}

func (*StubObjectStorageDriver) WriteObject(ctx context.Context, ns, key string, offset int64, data []byte) (int, error) {
	return 0, nil
}

func (*StubObjectStorageDriver) DeleteObject(ctx context.Context, ns, key string, force bool) error {
	return nil
}

func (*StubObjectStorageDriver) ListObjects(ctx context.Context, ns, key string) ([]*data.FileStat, error) {
	return nil, nil
}

func (*StubObjectStorageDriver) HeadObject(ctx context.Context, ns, key string) (*data.FileStat, error) {
	return nil, nil
}

func (*StubObjectStorageDriver) TruncateObject(ctx context.Context, ns, key string, size int64) error {
	return nil
}
