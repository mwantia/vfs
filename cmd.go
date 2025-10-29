package vfs

import (
	"context"
	"fmt"

	"github.com/mwantia/vfs/cmd"
)

func (vfs *virtualFileSystemImpl) RegisterCommand(cmd cmd.Command) error {
	if cmd == nil {
		return fmt.Errorf("command cannot be nil")
	}

	name := cmd.Name()
	if name == "" {
		return fmt.Errorf("command name cannot be empty")
	}

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if _, exists := vfs.cmds[name]; exists {
		return fmt.Errorf("command already registered: %s", name)
	}

	vfs.cmds[name] = cmd
	return nil
}

func (vfs *virtualFileSystemImpl) UnregisterCommand(name string) (bool, error) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	if _, exists := vfs.cmds[name]; !exists {
		return false, fmt.Errorf("command not found: %s", name)
	}

	delete(vfs.cmds, name)
	return true, nil
}

func (vfs *virtualFileSystemImpl) Execute(ctx context.Context, args ...string) (int, error) {
	if len(args) == 0 {
		return 1, fmt.Errorf("no command specified")
	}

	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	name := args[0]
	raw := args[1:]

	c, exists := vfs.cmds[name]
	if !exists {
		return 0, fmt.Errorf("command not found: %s", name)
	}

	flagSet := c.GetFlags()
	if flagSet == nil {
		flagSet = &cmd.CommandFlagSet{
			Flags: make(map[string]*cmd.CommandFlag),
		}
	}

	parser := cmd.NewParser(flagSet)
	parsedArgs, err := parser.Parse(raw)
	if err != nil {
		return 1, fmt.Errorf("parse error: %w", err)
	}

	return c.Execute(ctx, vfs, parsedArgs)
}
