package errors

import "fmt"

var (
	ErrMalformedBackendAddress        = fmt.Errorf("malformed backend address defined")
	ErrUnknownBackendProtocolAddress  = fmt.Errorf("unknown backend protocol address")
	ErrFailedObjectStorageBackendCast = fmt.Errorf("failed to parse backend address as object storage")
)
