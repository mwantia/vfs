package builtin

import (
	"io"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
)

func newConcatenateCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "cat <file>...",
		Short: "Concatenate and print files",
		Long:  "Read files and write their contents to stdout",
		Args:  cmd.MinimumArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()

			errs := errors.Errors{}

			for _, file := range args {
				stream, err := vfs.OpenFile(ctx.GetContext(), file, data.AccessModeRead)
				if err != nil {
					return exec.PrintError("cat: %s: %v\n", file, err)
				}

				if _, err := io.Copy(exec.Stdout, stream); err != nil {
					stream.Close()
					return exec.PrintError("cat: %s: %v\n", file, err)
				}

				errs.Add(stream.Close())
			}

			return errs.Errors()
		},
	}

	return cmd
}
