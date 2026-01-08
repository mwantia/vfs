package notification

import (
	"context"
	"time"

	"github.com/mwantia/vfs/mount/service"
)

type NotificationExtension interface {
	service.Service

	// Subscribe registers a handler to receive change events for a specific mount
	// The handler is called synchronously when events occur
	Subscribe(ctx context.Context, mp string, handler NotificationEventHandler) (EventSubscription, error)

	// Unsubscribe removes an active subscription
	Unsubscribe(ctx context.Context, sub EventSubscription) error

	// Emit sends a change event to all subscribers for the mount
	// This is called internally by mount implementations
	Emit(ctx context.Context, event NotificationEvent) error

	// GetHistory returns events since the given timestamp for a mount point
	// Useful for catching up on missed events
	GetHistory(ctx context.Context, mp string, since time.Time) ([]NotificationEvent, error)

	// ClearHistory clears all stored events for a mount point
	ClearHistory(ctx context.Context, mp string) error
}

// EventSubscription represents an active subscription to change events
type EventSubscription interface {
	// ID returns a unique identifier for this subscription
	ID() string
	// Unsubscribe stops receiving events
	Unsubscribe(ctx context.Context) error
}
