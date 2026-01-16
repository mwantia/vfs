package builtin

import (
	"strings"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/data"
)

func newTouchCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "touch <file>",
		Short: "Create an empty file",
		Long:  "Create an empty file or update the timestamp of an existing file",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()

			file := strings.TrimSpace(args[0])

			stream, err := vfs.OpenFile(ctx.GetContext(), file, data.AccessModeCreate|data.AccessModeWrite)
			if err != nil {
				return exec.PrintError("touch: %s: %v\n", file, err)
			}

			return stream.Close()
		},
	}

	return cmd
}
