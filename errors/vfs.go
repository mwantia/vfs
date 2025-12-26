package errors

import "fmt"

var (
	ErrRootNotMounted      = fmt.Errorf("virtual mountpoint for root ('/') not mounted")
	ErrRootNotHealthy      = fmt.Errorf("virtual mountpoint root is marked as unhealthy")
	ErrInvalidRootShutdown = fmt.Errorf("failed to shutdown virtual filesystem due to an invalid root")
)
