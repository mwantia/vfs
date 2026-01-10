package file

import "github.com/mwantia/vfs/mount/service"

func init() {
	// Register "file" as the primary scheme
	service.RegisterDriver(DriverName, func(uri *service.Uri) (service.Driver, error) {
		return NewFileDriver(uri)
	})
}
