package consul

import (
	"github.com/mwantia/vfs/mount/service"
)

func init() {
	service.RegisterDriver(DriverName, func(uri *service.Uri) (service.Driver, error) {
		return NewConsulMonolithDriver(uri, false)
	})
	service.RegisterDriver(DriverName+"s", func(uri *service.Uri) (service.Driver, error) {
		return NewConsulMonolithDriver(uri, true)
	})
}
