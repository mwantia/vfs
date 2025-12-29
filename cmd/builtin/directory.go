package builtin

import (
	"fmt"
	"path/filepath"

	"github.com/mwantia/vfs/cmd"
)

func lsCmd() *cmd.Command {
	c := &cmd.Command{
		Use:   "ls [path]",
		Short: "List directory contents",
		Long:  "List the contents of a directory",
		Args:  cmd.MaximumArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()

			// Default to current directory
			path := vfs.GetContext().GetCurrentDirectory()
			if len(args) > 0 {
				path = args[0]
			}

			// Convert relative to absolute
			if !filepath.IsAbs(path) {
				path = filepath.Join(vfs.GetContext().GetCurrentDirectory(), path)
			}

			// Read directory
			entries, err := vfs.ReadDirectory(ctx, path)
			if err != nil {
				fmt.Fprintf(execCtx.Stderr, "ls: %s: %v\n", path, err)
				return err
			}

			// Check for long format flag
			longFormat, _ := c.Flags().GetBool("long")

			// Print entries
			for _, entry := range entries {
				if longFormat {
					fmt.Fprintf(execCtx.Stdout, "%s %8d %s %s\n",
						entry.Mode, entry.Size,
						entry.ModifyTime.Format("Jan 02 15:04"),
						entry.Key)
				} else {
					fmt.Fprintf(execCtx.Stdout, "%s\n", entry.Key)
				}
			}

			return nil
		},
	}

	c.Flags().Bool("long", "l", false, "Use long listing format")
	return c
}

func mkdirCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "mkdir <directory>",
		Short: "Create a directory",
		Long:  "Create a new directory at the specified path",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()
			path := args[0]

			if err := vfs.CreateDirectory(ctx, path); err != nil {
				fmt.Fprintf(execCtx.Stderr, "mkdir: %s: %v\n", path, err)
				return err
			}

			return nil
		},
	}
}

func rmdirCmd() *cmd.Command {
	c := &cmd.Command{
		Use:   "rmdir <directory>",
		Short: "Remove a directory",
		Long:  "Remove an empty directory",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()
			path := args[0]

			// Check for force flag
			force, _ := c.Flags().GetBool("force")

			if err := vfs.RemoveDirectory(ctx, path, force); err != nil {
				fmt.Fprintf(execCtx.Stderr, "rmdir: %s: %v\n", path, err)
				return err
			}

			return nil
		},
	}

	c.Flags().Bool("force", "f", false, "Force removal (recursive)")
	return c
}

func cdCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "cd <directory>",
		Short: "Change current directory",
		Long:  "Change the current working directory",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()
			path := args[0]

			// Make path absolute if relative
			if !filepath.IsAbs(path) {
				path = filepath.Join(vfs.GetContext().GetCurrentDirectory(), path)
			}

			// Validate path exists and is a directory
			stat, err := vfs.StatMetadata(ctx, path)
			if err != nil {
				fmt.Fprintf(execCtx.Stderr, "cd: %s: %v\n", path, err)
				return err
			}

			if !stat.Mode.IsDir() {
				fmt.Fprintf(execCtx.Stderr, "cd: %s: not a directory\n", path)
				return fmt.Errorf("not a directory: %s", path)
			}

			// Update current directory
			vfs.GetContext().SetCurrentDirectory(path)
			return nil
		},
	}
}

func pwdCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "pwd",
		Short: "Print working directory",
		Long:  "Print the current working directory",
		Args:  cmd.NoArgsValidator{},
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			execCtx := vfs.GetExecutionContext()
			fmt.Fprintf(execCtx.Stdout, "%s\n", vfs.GetContext().GetCurrentDirectory())
			return nil
		},
	}
}
