package stub

import (
	"github.com/mwantia/vfs/mount/service"
)

var (
	DriverName = "stub"
	DriverType = service.ServiceTypeObjectStorage
)

type StubObjectStorageDriver struct {
}
