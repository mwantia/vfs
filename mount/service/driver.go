package service

import (
	"context"
)

// Lifecycle presents a low-level interface to control and
// validate the current lifecycle of the responsible driver.
type Lifecycle interface {
	// Name returns the hardcoded identifier set to this driver.
	Name() string

	// Health returns the current healthy state of the driver.
	// It indicates if all connections are performing successfully or not.
	Health() (bool, error)

	// IsBusy
	IsBusy() bool

	// GetCapabilities returns a list of capabilities supported by this driver.
	// This combines capabilities used by both object-storage and metadata.
	GetCapabilities() *DriverCapabilities
}

// Driver
type Driver interface {
	// Lifecycle
	Lifecycle

	// OpenDriver
	OpenDriver(ctx context.Context) error

	// CloseDriver
	CloseDriver(ctx context.Context) error

	// GetObjectStorageService
	GetObjectStorageService() ObjectStorageService

	// GetMetadataService
	GetMetadataService() MetadataService

	// GetExtensionService
	GetExtensionService(ext ServiceExtension) Service
}
