package vfs

import (
	"context"
	"io"
)

// RegisterCommand
func (vfs *virtualFileSystemImpl) RegisterCommand(cmd any) error {
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
	_, err := validateContext(ctx)
	if err != nil {
		return 0, err
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	return 0, nil
}

func (vfs *virtualFileSystemImpl) initBuiltinCommands() error {
	return nil
}
