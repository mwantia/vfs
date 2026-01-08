package notification

import (
	"context"
	"time"
)

// NotificationType describes the type of change that occurred
type NotificationType string

const (
	NotificationTypeFileOpened NotificationType = "file_opened"
	NotificationTypeFileClosed NotificationType = "file_closed"
	NotificationTypeRead       NotificationType = "read"
	NotificationTypeCreated    NotificationType = "created"
	NotificationTypeModified   NotificationType = "modified"
	NotificationTypeDeleted    NotificationType = "deleted"
	NotificationTypeRenamed    NotificationType = "renamed"
)

// NotificationEvent represents a single change event in the VFS
type NotificationEvent struct {
	// Absolute VFS path to the changed resource
	AbsolutePath string `json:"absolute_path"`
	// Relative path as seen by the mount
	RelativePath string `json:"relative_path"`
	// Mount point where the change occurred
	MountPoint string `json:"mount_point"`
	// Type of change
	Type NotificationType `json:"type"`
	// Timestamp when the change was detected
	Timestamp time.Time `json:"timestamp"`
	// File size (if applicable)
	Size int64 `json:"size,omitempty"`
	// Whether this is a directory
	IsDirectory bool `json:"is_directory"`
	// Optional: additional metadata (e.g., old path for rename)
	Metadata map[string]string `json:"metadata,omitempty"`
}

// NotificationEventHandler is called when a change event occurs
type NotificationEventHandler func(ctx context.Context, event NotificationEvent) error
