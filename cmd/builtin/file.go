package builtin

import (
	"fmt"
	"io"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/data"
)

func catCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "cat <file>...",
		Short: "Concatenate and print files",
		Long:  "Read files and write their contents to stdout",
		Args:  cmd.MinimumArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()

			for _, file := range args {
				stream, err := vfs.OpenFile(ctx, file, data.AccessModeRead)
				if err != nil {
					fmt.Fprintf(execCtx.Stderr, "cat: %s: %v\n", file, err)
					return err
				}

				if _, err := io.Copy(execCtx.Stdout, stream); err != nil {
					stream.Close()
					fmt.Fprintf(execCtx.Stderr, "cat: %s: %v\n", file, err)
					return err
				}

				stream.Close()
			}

			return nil
		},
	}
}

func teeCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "tee <file>...",
		Short: "Read from stdin and write to files and stdout",
		Long:  "Copy stdin to each specified file and also to stdout",
		Args:  cmd.MinimumArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()

			// Open all output files
			var writers []io.Writer
			var closers []io.Closer

			writers = append(writers, execCtx.Stdout)

			for _, file := range args {
				stream, err := vfs.OpenFile(ctx, file, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeTrunc)
				if err != nil {
					fmt.Fprintf(execCtx.Stderr, "tee: %s: %v\n", file, err)
					// Close already opened files
					for _, c := range closers {
						c.Close()
					}
					return err
				}
				writers = append(writers, stream)
				closers = append(closers, stream)
			}

			// Copy stdin to all writers
			multiWriter := io.MultiWriter(writers...)
			if _, err := io.Copy(multiWriter, execCtx.Stdin); err != nil {
				fmt.Fprintf(execCtx.Stderr, "tee: %v\n", err)
				for _, c := range closers {
					c.Close()
				}
				return err
			}

			// Close all files
			for _, c := range closers {
				c.Close()
			}

			return nil
		},
	}
}

func touchCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "touch <file>",
		Short: "Create an empty file",
		Long:  "Create an empty file or update the timestamp of an existing file",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()
			file := args[0]

			stream, err := vfs.OpenFile(ctx, file, data.AccessModeCreate|data.AccessModeWrite)
			if err != nil {
				fmt.Fprintf(execCtx.Stderr, "touch: %s: %v\n", file, err)
				return err
			}
			defer stream.Close()

			return nil
		},
	}
}

func mvCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "mv <source> <destination>",
		Short: "Move/rename files",
		Long:  "Move or rename a file from source to destination",
		Args:  cmd.ExactArgsValidator(2),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()
			source := args[0]
			dest := args[1]

			// Use VFS rename operation
			if err := vfs.Rename(ctx, source, dest); err != nil {
				fmt.Fprintf(execCtx.Stderr, "mv: %s -> %s: %v\n", source, dest, err)
				return err
			}

			return nil
		},
	}
}
