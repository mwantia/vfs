package errors

import "fmt"

var (
	ErrInvalidRootOperation   = fmt.Errorf("failed to perform operation due to an invalid root mount")
	ErrOperationNotSupported  = fmt.Errorf("operation is not supported by the service")
	ErrAccessModeNotAllowed   = fmt.Errorf("access mode is not allowed by the service")
	ErrCapabilityNotAvailable = fmt.Errorf("required capability is not available")
	ErrPathTooLong            = fmt.Errorf("path exceeds maximum length constraint")
	ErrPathTooDeep            = fmt.Errorf("path exceeds maximum depth constraint")
	ErrObjectTooLarge         = fmt.Errorf("object size exceeds maximum size constraint")
	ErrObjectTooSmall         = fmt.Errorf("object size is below minimum size constraint")
)
