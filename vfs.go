package vfs

import (
	"context"
	"sync"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/errors"
	"github.com/mwantia/vfs/log"
	"github.com/mwantia/vfs/mount"
)

type virtualFileSystemImpl struct {
	mu       sync.RWMutex
	shutdown bool

	log  *log.Logger
	root mount.MountPoint

	// Command system
	cmdContext *cmd.CommandContext
	rootCmd    *cmd.Command
	execCtx    *cmd.ExecutionContext
}

func NewVirtualFileSystem(opts ...VirtualFileSystemOption) (VirtualFileSystem, error) {
	options := newDefaultVirtualFileSystemOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	vfs := &virtualFileSystemImpl{
		log:        log.NewLogger("vfs", options.LogLevel, options.LogFile, options.NoTerminalLog),
		cmdContext: cmd.NewCommandContext(),
		execCtx:    cmd.NewExecutionContext(),
	}

	if err := vfs.initBuiltinCommands(); err != nil {
		return nil, err
	}

	return vfs, nil
}

// validateContext ensures the context is valid and not canceled.
// If ctx is nil, returns context.Background().
// If ctx is canceled, returns the original context and its error.
// This prevents panics and early detection of canceled operations at the public API boundary.
func validateContext(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Check if context is already canceled/expired
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	return ctx, nil
}

// Shutdown unmounts all mounted filesystems and releases all resources.
// This should be called when shutting down the VFS to ensure proper cleanup.
// Mounts are unmounted in reverse order (deepest first) to avoid dependency issues.
func (vfs *virtualFileSystemImpl) Shutdown(ctx context.Context) error {
	ctx, err := validateContext(ctx)
	if err != nil {
		return err
	}

	// Precheck by using RLock within 'checkRootMount'
	root, err := vfs.checkRootMount()
	if err != nil {
		return errors.ErrInvalidRootShutdown
	}
	// Only create the lock afterwards, which should block all calls since
	// we traverse through all mountpoints to ensure that all are unmounted.
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	/* TODO :: Should we maybe use a similar approach as in 'IsBusy'?
	 *         Using a separate mutex with 'TryLock'. All calls check
	 *         if the filesystem is "Busy" with a global operation,
	 *         like 'Shutdown' and either wait or fail its own call. */
	vfs.shutdown = true
	return root.Shutdown(ctx)
}

// setRootMountPoint
func (vfs *virtualFileSystemImpl) setRootMountPoint(root mount.MountPoint) error {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	// TODO :: Perform additional validation for root?
	vfs.root = root
	return nil
}

// checkRootMount
func (vfs *virtualFileSystemImpl) checkRootMount() (mount.MountPoint, error) {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()
	/* TODO :: Do we only need to sync root mount checks?
	 *         We technically have to keep locks within 'vfs' short.
	 *         To allow for concurrent access to multiple mountpoints,
	 *         it should restrict these calls as little as possible. */
	if vfs.root == nil {
		return nil, errors.ErrRootNotMounted
	}

	if !vfs.root.Health() {
		return nil, errors.ErrRootNotHealthy
	}

	return vfs.root, nil
}

// GetContext returns the command context
func (vfs *virtualFileSystemImpl) GetContext() *cmd.CommandContext {
	return vfs.cmdContext
}

// GetExecutionContext returns the execution context
func (vfs *virtualFileSystemImpl) GetExecutionContext() *cmd.ExecutionContext {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()
	return vfs.execCtx
}

// SetExecutionContext temporarily sets the execution context (used by executor)
func (vfs *virtualFileSystemImpl) SetExecutionContext(execCtx *cmd.ExecutionContext) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	vfs.execCtx = execCtx
}
