package cmd

import (
	"context"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/builder"
)

// API provides the interface that commands use to interact with VFS.
// It includes all VFS operations plus command context management.
type API interface {
	// VFS Lifecycle
	Shutdown(ctx context.Context) error

	// Mount Operations
	Mount(ctx context.Context, path string, steps ...builder.MountStep) error
	Unmount(ctx context.Context, path string, force bool) error
	ListMountSpecs(ctx context.Context) (map[string]builder.MountSpecifications, error)
	GetMountSpec(ctx context.Context, path string) (builder.MountSpecifications, error)
	UpdateMountSpec(ctx context.Context, path string, update builder.MountSpecUpdate) (builder.MountSpecifications, error)

	// File I/O Operations
	OpenFile(ctx context.Context, path string, flags data.AccessMode, opts ...mount.MountStreamerOption) (mount.Streamer, error)
	CloseFile(ctx context.Context, path string, force bool) error
	ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error)
	WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error)
	UnlinkFile(ctx context.Context, path string) error

	// Metadata Operations
	StatMetadata(ctx context.Context, path string) (data.Metadata, error)
	LookupMetadata(ctx context.Context, path string) (bool, error)

	// Directory Operations
	ReadDirectory(ctx context.Context, path string) ([]data.Metadata, error)
	CreateDirectory(ctx context.Context, path string) error
	RemoveDirectory(ctx context.Context, path string, force bool) error

	// File Movement
	Rename(ctx context.Context, oldPath string, newPath string) error

	// Command Context
	GetContext() *CommandContext

	// Execution Context (for accessing stdin/stdout/stderr in commands)
	GetExecutionContext() *ExecutionContext
	SetExecutionContext(*ExecutionContext)
}

type ArgsValidator interface {
	Validate(args []string) error
}
