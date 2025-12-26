package direct

import "github.com/mwantia/vfs/mount/service"

func init() {
	// Register "file" as the primary scheme
	service.RegisterDriver(DriverName, func(uri *service.Uri) (service.Driver, error) {
		return NewDirectDriver(uri)
	})

	// Also register "direct" as an alias
	service.RegisterDriver("direct", func(uri *service.Uri) (service.Driver, error) {
		return NewDirectDriver(uri)
	})
}
