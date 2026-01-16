package builtin

import (
	"strings"

	"github.com/mwantia/vfs/cmd"
)

func newMoveCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "mv <source> <destination>",
		Short: "Move/rename files",
		Long:  "Move or rename a file from source to destination",
		Args:  cmd.ExactArgsValidator(2),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()

			source := strings.TrimSpace(args[0])
			destination := strings.TrimSpace(args[1])

			// Use VFS rename operation
			if err := vfs.Rename(ctx.GetContext(), source, destination); err != nil {
				return exec.PrintError("mv: %s -> %s: %v\n", source, destination, err)
			}

			return nil
		},
	}

	return cmd
}
