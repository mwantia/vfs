package builtin

import (
	"context"
	"fmt"
	pathpkg "path"

	"github.com/mwantia/vfs/cmd"
)

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
			if !pathpkg.IsAbs(path) {
				path = pathpkg.Join(vfs.GetContext().GetCurrentDirectory(), path)
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

func walkCmd() *cmd.Command {
	c := &cmd.Command{
		Use:   "walk [path]",
		Short: "Walk directory tree and populate metadata",
		Long:  "Recursively walk the directory tree from the specified path (or current directory) and ensure all metadata is loaded from object storage into metadata service. Useful for testing and populating metadata.",
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
			if !pathpkg.IsAbs(path) {
				path = pathpkg.Join(vfs.GetContext().GetCurrentDirectory(), path)
			}

			// Check for verbose flag
			verbose, _ := c.Flags().GetBool("verbose")

			// Walk the tree
			var fileCount, dirCount int
			err := walkTree(vfs, ctx, execCtx, path, verbose, &fileCount, &dirCount)
			if err != nil {
				fmt.Fprintf(execCtx.Stderr, "walk: %v\n", err)
				return err
			}

			// Print summary
			fmt.Fprintf(execCtx.Stdout, "\nWalk complete: %d files, %d directories\n", fileCount, dirCount)
			return nil
		},
	}

	c.Flags().Bool("verbose", "v", false, "Show each file and directory as it's processed")
	return c
}

func walkTree(vfs cmd.API, ctx context.Context, execCtx *cmd.ExecutionContext, path string, verbose bool, fileCount, dirCount *int) error {
	// Stat the path to ensure metadata is loaded from object storage
	stat, err := vfs.StatMetadata(ctx, path)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	// If verbose, show progress
	if verbose {
		if stat.Mode.IsDir() {
			fmt.Fprintf(execCtx.Stdout, "d %s\n", path)
		} else {
			fmt.Fprintf(execCtx.Stdout, "f %s\n", path)
		}
	}

	// If not a directory, just count and return
	if !stat.Mode.IsDir() {
		*fileCount++
		return nil
	}

	*dirCount++

	// Read directory entries
	entries, err := vfs.ReadDirectory(ctx, path)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	// Recursively walk each entry
	for _, entry := range entries {
		if err := walkTree(vfs, ctx, execCtx, entry.Key, verbose, fileCount, dirCount); err != nil {
			return err
		}
	}

	return nil
}
