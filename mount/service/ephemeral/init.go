package ephemeral

import "github.com/mwantia/vfs/mount/service"

func init() {
	service.RegisterDriver(DriverName, func(uri *service.Uri) (service.Driver, error) {
		return NewEphemeralMonolithDriver(uri), nil
	})
}
