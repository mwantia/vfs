package backend

import "context"

// Backend is used as lifecycle entrypoint for other backend implementations.
type Backend interface {
	// Name returns the identifier name defined for this backend
	Name() string
	// Open is part of the lifecycle behavious and gets called when opening this backend.
	Open(ctx context.Context) error
	// Close is part of the lifecycle behaviour and gets called when closing this backend.
	Close(ctx context.Context) error

	// GetCapabilities returns a list of capabilities supported by this backend.
	GetCapabilities() *BackendCapabilities
}
