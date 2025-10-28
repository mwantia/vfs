package errors

func InvalidPath(err error, path string) error {
	return newError(err, "invalid path '%s' detected", path)
}

func PathNotMounted(err error, path string) error {
	return newError(err, "path '%s' not mounted", path)
}

func PathAlreadyMounted(err error, path string) error {
	return newError(err, "path '%s' already mounted", path)
}

func PathMountBusy(err error, path string) error {
	return newError(err, "mount point '%s' busy", path)
}

func PathMountNestingDenied(err error, path string) error {
	return newError(err, "mount nesting denied by parent mount '%s'", path)
}
