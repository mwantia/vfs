package backend

import "github.com/mwantia/vfs/pkg/context"

// Backend is used as lifecycle entrypoint for other backend implementations.
type Backend interface {
	// Name returns the identifier name defined for this backend
	Name() string
	// Open is part of the lifecycle behavious and gets called when opening this backend.
	OpenBackend(ctx context.TraversalContext) error
	// Close is part of the lifecycle behaviour and gets called when closing this backend.
	CloseBackend(ctx context.TraversalContext) error
	// IsBusy determines and returns if it's safe to close this backend.
	IsBusy() bool
	// Health returns the basic and fastest result to check the lifecycle and availablility of this backend.
	Health() bool
	// GetCapabilities returns a list of capabilities supported by this backend.
	GetCapabilities() *Capabilities
}
