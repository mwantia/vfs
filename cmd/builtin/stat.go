package builtin

import (
	"encoding/json"
	"fmt"

	"github.com/mwantia/vfs/cmd"
)

func newStatCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "stat <path>",
		Short: "Display file metadata",
		Long:  "Display detailed metadata for the specified file or directory",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			exec := vfs.GetExecutionContext()
			path := args[0]

			meta, err := vfs.StatMetadata(ctx, path)
			if err != nil {
				return exec.PrintError("stat: %s: %v\n", path, err)
			}

			if j, _ := c.Flags().GetBool("json"); j {
				buffer, err := json.MarshalIndent(meta, "", "  ")
				if err != nil {
					return err
				}

				return exec.PrintOutput("%s\n", string(buffer))
			}

			fmt.Fprintf(exec.Stdout, "  File: %s\n", meta.Key)
			fmt.Fprintf(exec.Stdout, "  Size: %d\n", meta.Size)
			fmt.Fprintf(exec.Stdout, "  Mode: %s\n", meta.Mode)
			fmt.Fprintf(exec.Stdout, "  Type: %s\n", getFileType(meta.Mode))
			if !meta.CreateTime.IsZero() {
				fmt.Fprintf(exec.Stdout, "Create: %s\n", meta.CreateTime.Format("2006-01-02 15:04:05"))
			}
			if !meta.ModifyTime.IsZero() {
				fmt.Fprintf(exec.Stdout, "Modify: %s\n", meta.ModifyTime.Format("2006-01-02 15:04:05"))
			}
			if !meta.AccessTime.IsZero() {
				fmt.Fprintf(exec.Stdout, "Access: %s\n", meta.AccessTime.Format("2006-01-02 15:04:05"))
			}
			fmt.Fprintf(exec.Stdout, "   UID: %d\n", meta.UID)
			fmt.Fprintf(exec.Stdout, "   GID: %d\n", meta.GID)
			fmt.Fprintf(exec.Stdout, "  Type: %s\n", meta.ContentType)
			fmt.Fprintf(exec.Stdout, "  ETag: %s\n", meta.ETag)

			return nil
		},
	}

	cmd.Flags().Bool("json", "j", false, "Output metadata as json")

	return cmd
}
