package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/cmd/builtin"
)

// RegisterCommand registers a command with the VFS
func (vfs *virtualFileSystemImpl) RegisterCommand(c any) error {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	command, ok := c.(*cmd.Command)
	if !ok {
		return fmt.Errorf("invalid command type: expected *cmd.Command")
	}

	if vfs.rootCmd == nil {
		return fmt.Errorf("root command not initialized")
	}

	return vfs.rootCmd.AddCommand(command)
}

// UnregisterCommand removes a command from the VFS
func (vfs *virtualFileSystemImpl) UnregisterCommand(name string) (bool, error) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if vfs.rootCmd == nil || vfs.rootCmd.Children(name) == nil {
		return false, nil
	}

	// Access children map directly to delete
	// Note: This requires children to be accessible, which might need a method
	children := vfs.rootCmd.GetAllChildren()
	if _, exists := children[name]; !exists {
		return false, nil
	}

	// Remove from root's children map
	// This is a bit hacky - ideally Command should have a RemoveCommand method
	delete(vfs.rootCmd.GetAllChildren(), name)
	return true, nil
}

// Execute runs a command string
func (vfs *virtualFileSystemImpl) Execute(ctx context.Context, writer io.Writer, args ...string) (int, error) {
	// Use default stdin/stderr with custom stdout
	return vfs.ExecuteWithStreams(ctx, os.Stdin, writer, os.Stderr, args...)
}

// ExecuteWithStreams runs a command string with full control over stdin, stdout, and stderr
func (vfs *virtualFileSystemImpl) ExecuteWithStreams(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, args ...string) (int, error) {
	ctx, err := validateContext(ctx)
	if err != nil {
		return 1, err
	}

	vfs.mu.RLock()
	rootCmd := vfs.rootCmd
	execCtx := vfs.execCtx
	vfs.mu.RUnlock()

	if rootCmd == nil {
		return 1, fmt.Errorf("command system not initialized")
	}

	// Set streams in execution context
	if execCtx != nil {
		if stdin != nil {
			execCtx.Stdin = stdin
		}
		if stdout != nil {
			execCtx.Stdout = stdout
		}
		if stderr != nil {
			execCtx.Stderr = stderr
		}
	}

	// Join args into command string
	cmdString := strings.Join(args, " ")

	// Execute command
	return rootCmd.Execute(ctx, vfs, cmdString)
}

// initBuiltinCommands initializes the builtin command set
func (vfs *virtualFileSystemImpl) initBuiltinCommands() error {
	vfs.rootCmd = &cmd.Command{
		Use:   "vfs",
		Short: "VFS root command",
		Long:  "Virtual File System root command - manages all VFS operations",
	}

	// Register all builtin commands
	if err := builtin.RegisterBuiltins(vfs.rootCmd); err != nil {
		return fmt.Errorf("failed to register builtin commands: %w", err)
	}

	return nil
}
