package errors

import "errors"

var (
	ErrRootNotMounted = errors.New("virtual mountpoint for root ('/') not mounted")
	ErrRootNotHealthy = errors.New("virtual mountpoint root is marked as unhealthy")
)
