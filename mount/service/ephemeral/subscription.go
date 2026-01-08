package ephemeral

import (
	"context"

	"github.com/mwantia/vfs/mount/extensions/notification"
)

type ephemeralSubscription struct {
	id         string
	mountpoint string
	handler    notification.NotificationEventHandler
}

func (s *ephemeralSubscription) Unsubscribe(ctx context.Context) error {
	return nil
}

func (s *ephemeralSubscription) ID() string {
	return s.id
}
