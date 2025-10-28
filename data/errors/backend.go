package errors

func BackendUnsupported(err error, name string) error {
	return newError(err, "backend capability unsupported for '%s'", name)
}

func BackendIncompatible(err error, name string) error {
	return newError(err, "backend incompatible for '%s'", name)
}
