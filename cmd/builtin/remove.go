package builtin

import (
	"fmt"
	pathpkg "path"
	"strings"

	"github.com/mwantia/vfs/cmd"
)

func newRemoveCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "rm <file>...",
		Short: "Removal of files/directories",
		Long:  "Remove (unlink) the specified files or directories",
		Args:  cmd.MinimumArgsValidator(1),
		Run: func(vfs cmd.API, cmd *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()

			for _, path := range args {
				absolutePath := strings.TrimSpace(path)
				if absolutePath == "" {
					continue
				}

				if !pathpkg.IsAbs(absolutePath) {
					current := ctx.GetCurrentDirectory()
					absolutePath = pathpkg.Join(current, absolutePath)
				}

				meta, err := vfs.StatMetadata(ctx.GetContext(), absolutePath)
				if err != nil {
					exec.PrintError("rm: %s: %v\n", path, err)
					return err
				}

				if meta.Mode.IsDir() {
					if recursive, _ := cmd.Flags().GetBool("recursive"); !recursive {
						return fmt.Errorf("rm: cannot remove '%s': is a directory", path)
					}

					force, _ := cmd.Flags().GetBool("force")

					if err := vfs.RemoveDirectory(ctx.GetContext(), absolutePath, force); err != nil {
						return err
					}

					return nil
				}

				return vfs.UnlinkFile(ctx.GetContext(), absolutePath)
			}

			return nil
		},
	}

	cmd.Flags().Bool("force", "f", false, "Implicit directories need to be forcefully deleted")
	cmd.Flags().Bool("recursive", "r", false, "Recursively remove contents from directories")

	return cmd
}
