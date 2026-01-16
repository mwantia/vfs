package builtin

import (
	pathpkg "path"
	"strings"

	"github.com/mwantia/vfs/cmd"
)

func newChangeDirectoryCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "cd <directory>",
		Short: "Change current directory",
		Long:  "Change the current working directory",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()

			path := strings.TrimSpace(args[0])
			pwd := ctx.GetCurrentDirectory()
			// Make path absolute if relative
			if !pathpkg.IsAbs(path) {
				path = pathpkg.Join(pwd, path)
			}
			// Validate path exists and is a directory
			stat, err := vfs.StatMetadata(ctx.GetContext(), path)
			if err != nil {
				return exec.PrintError("cd: %s: %v\n", path, err)
			}

			if !stat.Mode.IsDir() {
				return exec.PrintError("not a directory: %s", path)
			}
			// Update current directory
			ctx.SetCurrentDirectory(path)
			return nil
		},
	}

	return cmd
}

func newMakeDirectoryCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "mkdir <directory>",
		Short: "Create a directory",
		Long:  "Create a new directory at the specified path",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()
			path := strings.TrimSpace(args[0])

			if err := vfs.CreateDirectory(ctx.GetContext(), path); err != nil {
				return exec.PrintError("mkdir: %s: %v\n", path, err)
			}

			return nil
		},
	}

	return cmd
}
