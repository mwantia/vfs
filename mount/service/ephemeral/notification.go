package ephemeral

import (
	"context"
	"fmt"
	"time"

	"github.com/mwantia/vfs/mount/extensions/notification"
	"github.com/mwantia/vfs/mount/service"
)

func (s *EphemeralNotificationExtensionService) GetLifecycle() service.Lifecycle {
	return s.driver
}

func (s *EphemeralNotificationExtensionService) Subscribe(ctx context.Context, mp string, handler notification.NotificationEventHandler) (notification.EventSubscription, error) {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()

	s.hub.subscriptionIdx++
	id := fmt.Sprintf("%s-%d", mp, s.hub.subscriptionIdx)

	sub := &ephemeralSubscription{
		id:         id,
		mountpoint: mp,
		handler:    handler,
	}

	s.hub.subscriptions[id] = sub
	return sub, nil
}

func (s *EphemeralNotificationExtensionService) Unsubscribe(ctx context.Context, sub notification.EventSubscription) error {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()

	id := sub.ID()
	delete(s.hub.subscriptions, id)

	return nil
}

func (s *EphemeralNotificationExtensionService) Emit(ctx context.Context, event notification.NotificationEvent) error {
	if event.MountPoint == "" {
		return fmt.Errorf("event mount point cannot be empty")
	}

	s.hub.mu.Lock()

	// Store event in history (with rotation)
	s.hub.events = append(s.hub.events, event)
	if len(s.hub.events) > s.hub.maxHistorySize {
		// Keep only the most recent events
		s.hub.events = s.hub.events[len(s.hub.events)-s.hub.maxHistorySize:]
	}

	// Collect subscriptions for this mount point (copy to avoid holding lock during handler calls)
	var handlersToCall []*ephemeralSubscription
	for _, sub := range s.hub.subscriptions {
		if sub.mountpoint == event.MountPoint {
			handlersToCall = append(handlersToCall, sub)
		}
	}

	s.hub.mu.Unlock()

	// Call all handlers synchronously (outside the lock to avoid deadlocks)
	for _, sub := range handlersToCall {
		if err := sub.handler(ctx, event); err != nil {
			// Continue calling other handlers even if one fails
			// In production, you'd want to log this error
			_ = err
		}
	}

	return nil
}

func (s *EphemeralNotificationExtensionService) GetHistory(ctx context.Context, mp string, since time.Time) ([]notification.NotificationEvent, error) {
	s.hub.mu.RLock()
	defer s.hub.mu.RUnlock()

	var result []notification.NotificationEvent
	for _, event := range s.hub.events {
		if event.MountPoint == mp && event.Timestamp.After(since) {
			result = append(result, event)
		}
	}

	return result, nil
}

func (s *EphemeralNotificationExtensionService) ClearHistory(ctx context.Context, mp string) error {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()

	if mp == "" {
		// Clear all events
		s.hub.events = s.hub.events[:0]
	} else {
		// Clear events only for specific mount point
		filtered := make([]notification.NotificationEvent, 0, len(s.hub.events))
		for _, event := range s.hub.events {
			if event.MountPoint != mp {
				filtered = append(filtered, event)
			}
		}
		s.hub.events = filtered
	}

	return nil
}
