package pkg

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/log"
	tctx "github.com/mwantia/vfs/pkg/context"
	"github.com/mwantia/vfs/pkg/errors"
	"github.com/mwantia/vfs/pkg/mount"
)

type virtualFileSystemImpl struct {
	mu sync.RWMutex

	log  *log.Logger
	root *mount.MountPoint
	cmds map[string]cmd.Command
}

func NewVirtualFileSystem(opts ...VirtualFileSystemOption) (VirtualFileSystem, error) {
	options := newDefaultVirtualFileSystemOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	vfs := &virtualFileSystemImpl{
		log:  log.NewLogger("vfs", options.LogLevel, options.LogFile, options.NoTerminalLog),
		cmds: make(map[string]cmd.Command),
	}

	if err := vfs.initBuiltinCommands(); err != nil {
		return nil, err
	}

	return vfs, nil
}

// Shutdown unmounts all mounted filesystems and releases all resources.
// This should be called when shutting down the VFS to ensure proper cleanup.
// Mounts are unmounted in reverse order (deepest first) to avoid dependency issues.
func (vfs *virtualFileSystemImpl) Shutdown(ctx context.Context) error {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	root, err := vfs.checkRootMount()
	if err != nil {
		return fmt.Errorf("failed to shutdown virtual filesystem: %w", err)
	}

	return root.Shutdown(tctx.WithAbsolute(ctx, "/"))
}

// RegisterCommand
func (vfs *virtualFileSystemImpl) RegisterCommand(cmd cmd.Command) error {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	return nil
}

// UnregisterCommand
func (vfs *virtualFileSystemImpl) UnregisterCommand(name string) (bool, error) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	return false, nil
}

// Execute runs a command with the given arguments, writing output to the provided writer
func (vfs *virtualFileSystemImpl) Execute(ctx context.Context, writer io.Writer, args ...string) (int, error) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	return 0, nil
}

func (vfs *virtualFileSystemImpl) initBuiltinCommands() error {
	return nil
}

func (vfs *virtualFileSystemImpl) checkRootMount() (*mount.MountPoint, error) {
	if vfs.root == nil {
		return nil, errors.ErrRootNotMounted
	}

	if !vfs.root.Health() {
		return nil, errors.ErrRootNotHealthy
	}

	return vfs.root, nil
}
