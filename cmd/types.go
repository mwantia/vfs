package cmd

import (
	"context"
	"io"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/builder"
)

type Command struct {
	// Use is the one-line usage message.
	Use string

	// Short is the shortest description shown in help.
	Short string

	// Long is the longest description shown in help <command>
	Long string

	// Args defines validation for positional arguments.
	Args ArgsValidator

	Run func(vfs API, cmd *Command, args []string) error

	children map[string]*Command // Children
	parent   *Command            // Parent command (is set automatically when added as subcommand)
	flags    *FlagSet            // Flags for this command
	out      io.Writer           // Output writer for messages (set by VFS)
}

type Flag struct {
	Name  string // Long name (e.g., "output")
	Short string // Short name (e.g., "o")
	Usage string // Help text

	Required bool // Whether flag is required

	Type    string // "bool", "string", "int", "stringSlice"
	Changed bool
	Value   any
	Default any // Default value
}

type FlagSet struct {
	flags map[string]*Flag
}

type ArgsValidator interface {
	Validate(args []string) error
}

// API provides the interface that commands use to interact with VFS.
// It includes all VFS operations plus command context management.
type API interface {
	// VFS Lifecycle
	Shutdown(ctx context.Context) error

	// Mount Operations
	Mount(ctx context.Context, path string, steps ...builder.MountStep) error
	Unmount(ctx context.Context, path string, force bool) error

	// File I/O Operations
	OpenFile(ctx context.Context, path string, flags data.AccessMode) (mount.MountStreamer, error)
	CloseFile(ctx context.Context, path string, force bool) error
	ReadFile(ctx context.Context, path string, offset, size int64) ([]byte, error)
	WriteFile(ctx context.Context, path string, offset int64, buffer []byte) (int, error)
	UnlinkFile(ctx context.Context, path string) error

	// Metadata Operations
	StatMetadata(ctx context.Context, path string) (*data.Metadata, error)
	LookupMetadata(ctx context.Context, path string) (bool, error)

	// Directory Operations
	ReadDirectory(ctx context.Context, path string) ([]*data.Metadata, error)
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
