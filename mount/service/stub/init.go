package stub

import (
	"github.com/mwantia/vfs/mount/service"
)

func init() {
	service.RegisterDriver(DriverName, func(uri *service.Uri) (service.Driver, error) {
		return &StubObjectStorageDriver{}, nil
	})
}
