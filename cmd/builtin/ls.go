package builtin

import (
	"context"
	"fmt"

	"github.com/mwantia/vfs/cmd"
)

type LsCommand struct {
}

// Name returns the command identifier
func (ls *LsCommand) Name() string {
	return "ls"
}

// Description returns human-readable help text
func (ls *LsCommand) Description() string {
	return ""
}

// Usage returns a usage string for help (e.g. "ls -al [path]")
func (ls *LsCommand) Usage() string {
	return ""
}

// Execute runs the command with parsed arguments
// Returns exit code (0 = success) and error message
func (ls *LsCommand) Execute(ctx context.Context, api cmd.API, args *cmd.CommandArgs) (int, error) {
	return 0, fmt.Errorf("vfs: not implemented")
}

// GetFlags returns the flag set for this command (this is optional)
func (ls *LsCommand) GetFlags() *cmd.CommandFlagSet {
	return nil
}
