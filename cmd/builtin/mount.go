package builtin

import (
	"fmt"
	"strings"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/builder"
)

func mountCmd() *cmd.Command {
	c := &cmd.Command{
		Use:   "mount <uri> <path>",
		Short: "Mount a filesystem",
		Long:  "Mount a filesystem at the specified path using the given URI",
		Args:  cmd.ExactArgsValidator(2),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			path := strings.TrimSpace(args[0])
			uri := strings.TrimSpace(args[1])

			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()

			steps, err := mount.IdentifyMountSteps(ctx, uri)
			if err != nil {
				return err
			}

			// Check for metadata flag
			if metadata, ok := c.Flags().GetString("metadata"); ok && metadata != "" {
				steps = append(steps, builder.WithMetadata(metadata))
			}

			// Mount the filesystem
			if err := vfs.Mount(ctx, path, steps...); err != nil {
				fmt.Fprintf(execCtx.Stderr, "mount: failed to mount %s at %s: %v\n", uri, path, err)
				return err
			}

			fmt.Fprintf(execCtx.Stdout, "Mounted %s at %s\n", uri, path)
			return nil
		},
	}

	c.Flags().String("metadata", "m", "", "Metadata service URI")
	return c
}

func umountCmd() *cmd.Command {
	c := &cmd.Command{
		Use:   "umount <path>",
		Short: "Unmount a filesystem",
		Long:  "Unmount the filesystem at the specified path",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			path := args[0]

			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()

			// Check for force flag
			force, _ := c.Flags().GetBool("force")

			// Unmount the filesystem
			if err := vfs.Unmount(ctx, path, force); err != nil {
				fmt.Fprintf(execCtx.Stderr, "umount: failed to unmount %s: %v\n", path, err)
				return err
			}

			fmt.Fprintf(execCtx.Stdout, "Unmounted %s\n", path)
			return nil
		},
	}

	c.Flags().Bool("force", "f", false, "Force unmount even if busy")
	return c
}
