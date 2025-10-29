package errors

func BackendUnsupported(err error, name string) error {
	return newError(err, "backend capability unsupported for '%s'", name)
}

func BackendIncompatible(err error, name string) error {
	return newError(err, "backend incompatible for '%s'", name)
}

func BackendObjectTooSmall(err error, size, minSize int64) error {
	return newError(err, "object size %d bytes is below minimum allowed size %d bytes", size, minSize)
}

func BackendObjectTooLarge(err error, size, maxSize int64) error {
	return newError(err, "object size %d bytes exceeds maximum allowed size %d bytes", size, maxSize)
}
