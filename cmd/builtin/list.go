package builtin

import (
	"fmt"
	pathpkg "path"
	"path/filepath"
	"strings"

	"github.com/mwantia/vfs/cmd"
)

func newListDirectoryCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "ls [path]",
		Short: "List directory entries",
		Long:  "List the entries of a directory",
		Args:  cmd.MaximumArgsValidator(1),
		Run: func(vfs cmd.API, cmd *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()

			path := ctx.GetCurrentDirectory()
			if len(args) > 0 {
				path = strings.TrimSpace(args[0])
			}

			if !pathpkg.IsAbs(path) {
				current := ctx.GetCurrentDirectory()
				path = pathpkg.Join(current, path)
			}

			entries, err := vfs.ReadDirectory(ctx.GetContext(), path)
			if err != nil {
				return fmt.Errorf("ls: %s: %v", path, err)
			}

			all, _ := cmd.Flags().GetBool("all")
			longFormat, _ := cmd.Flags().GetBool("long-format")
			humanReadable, _ := cmd.Flags().GetBool("human-readable")

			for _, entry := range entries {
				// Ignore entries that start with '.' when --all is not defined
				if all || !strings.HasPrefix(entry.Key, ".") {
					if longFormat {
						size := formatSize(entry.Size, humanReadable)
						time := entry.ModifyTime.Format("Jan 02 15:04")
						name := filepath.Base(entry.Key)

						exec.PrintOutput("%s %8s %s %s\n", entry.Mode, size, time, name)
					} else {
						exec.PrintOutput("%s\n", entry.Key)
					}
				}
			}

			return nil
		},
	}

	cmd.Flags().Bool("all", "a", false, "List all entries (includes hidden files and folders)")
	cmd.Flags().Bool("human-readable", "h", false, "Display sizes in a human-readable format")
	cmd.Flags().Bool("long-format", "l", false, "Use long listing format")

	return cmd
}
