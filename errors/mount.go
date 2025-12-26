package errors

import "fmt"

var (
	ErrInvalidRootMount   = fmt.Errorf("failed to mount - invalid root mount")
	ErrInvalidRootUnmount = fmt.Errorf("failed to unmount - invalid root mount")
	ErrMountNestingDenied = fmt.Errorf("failed to mount - nesting denied")
	ErrMountIsReadonly    = fmt.Errorf("failed to process - mount is marked as readonly")
	ErrPathNotMounted     = fmt.Errorf("failed to unmount - path not mounted")
	ErrMountBusy          = fmt.Errorf("failed to unmount - mount is busy or has child mounts")
)
