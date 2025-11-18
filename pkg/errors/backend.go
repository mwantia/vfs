package errors

import "fmt"

var (
	ErrMalformedBackendAddress       = fmt.Errorf("malformed backend address defined")
	ErrUnknownBackendProtocolAddress = fmt.Errorf("unknown backend protocol address")
)
