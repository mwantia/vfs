package builtin

import (
	"strings"

	"github.com/mwantia/vfs/cmd"
)

func newPrintWorkingDirectoryCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "pwd",
		Short: "Print working directory",
		Long:  "Print the current working directory",
		Args:  cmd.NoArgsValidator{},
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()

			current := ctx.GetCurrentDirectory()
			return exec.PrintOutput("%s\n", current)
		},
	}

	return cmd
}

func newPrintfCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "printf [text]...",
		Short: "Print raw text to stdout",
		Long:  "Print the specified text to stdout",
		Args:  cmd.ArbitraryArgsValidator{},
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			exec := vfs.GetExecutionContext()
			return exec.PrintOutput("%s", strings.Join(args, " "))
		},
	}

	return cmd
}

func newEchoCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "echo [text]...",
		Short: "Print text to stdout",
		Long:  "Print the specified text to stdout",
		Args:  cmd.ArbitraryArgsValidator{},
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			exec := vfs.GetExecutionContext()
			return exec.PrintOutput("%s\n", strings.Join(args, " "))
		},
	}

	return cmd
}
