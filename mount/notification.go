package mount

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/mwantia/vfs/mount/extensions/notification"
	"github.com/mwantia/vfs/mount/service"
)

func (m *Mount) emitNotificationEvent(ctx context.Context, changeType notification.NotificationType, relativePath string, isDir bool, size int64) (bool, error) {
	ext, exists := m.Extensions[service.ServiceExtensionNotification]
	if !exists {
		return false, fmt.Errorf("notification extension doesn't exist")
	}

	notif, ok := ext.(notification.NotificationExtension)
	if !ok {
		return false, fmt.Errorf("failed to cast extension as 'NotificationExtension'")
	}

	absolutePath := filepath.Join(m.MountPoint, relativePath)
	if relativePath == "" || relativePath == "/" {
		absolutePath = m.MountPoint
	}

	ev := notification.NotificationEvent{
		AbsolutePath: absolutePath,
		RelativePath: relativePath,
		MountPoint:   m.MountPoint,
		Type:         changeType,
		Timestamp:    time.Now(),
		IsDirectory:  isDir,
		Size:         size,
	}

	go func() {
		emitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := notif.Emit(emitCtx, ev); err != nil {
			// In production, you'd want to log this
			fmt.Printf("WARNING: Failed to emit change event: %v\n", err)
		}
	}()

	return true, nil
}
