package builtin

import (
	"io"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
)

func newTeeCommand() *cmd.Command {
	return &cmd.Command{
		Use:   "tee [file]...",
		Short: "Read from stdin and write to files and stdout",
		Long:  "Copy stdin to each specified file and also to stdout",
		Args:  cmd.MinimumArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()
			// Open all output files
			var writers []io.Writer
			var closers []io.Closer

			writers = append(writers, exec.Stdout)

			for _, file := range args {
				stream, err := vfs.OpenFile(ctx.GetContext(), file, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeTrunc)
				if err != nil {
					// Close already opened files
					for _, c := range closers {
						c.Close()
					}

					return exec.PrintError("tee: %s: %v\n", file, err)
				}

				writers = append(writers, stream)
				closers = append(closers, stream)
			}
			// Copy stdin to all writers
			multiWriter := io.MultiWriter(writers...)
			if _, err := io.Copy(multiWriter, exec.Stdin); err != nil {
				for _, c := range closers {
					c.Close()
				}

				return exec.PrintError("tee: %v\n", err)
			}
			// Close all files
			errs := errors.Errors{}
			for _, c := range closers {
				errs.Add(c.Close())
			}

			return errs.Errors()
		},
	}
}
