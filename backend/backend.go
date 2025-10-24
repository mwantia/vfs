package backend

import "context"

// VirtualBackend is used as lifecycle entrypoint for other backend implementations.
type VirtualBackend interface {
	// Returns the identifier name defined for this backend
	GetName() string
	// Open is part of the lifecycle behavious and gets called when opening this backend.
	Open(ctx context.Context) error
	// Close is part of the lifecycle behaviour and gets called when closing this backend.
	Close(ctx context.Context) error

	// GetCapabilities returns a list of capabilities supported by this backend.
	GetCapabilities() *VirtualBackendCapabilities
}
