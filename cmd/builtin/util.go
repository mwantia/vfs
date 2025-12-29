package builtin

import (
	"fmt"
	"strings"

	"github.com/mwantia/vfs/cmd"
)

func echoCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "echo [text...]",
		Short: "Print text to stdout",
		Long:  "Print the specified text to stdout",
		Args:  cmd.ArbitraryArgsValidator{},
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			execCtx := vfs.GetExecutionContext()
			fmt.Fprintf(execCtx.Stdout, "%s\n", strings.Join(args, " "))
			return nil
		},
	}
}

func statCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "stat <path>",
		Short: "Display file metadata",
		Long:  "Display detailed metadata for the specified file or directory",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()
			path := args[0]

			stat, err := vfs.StatMetadata(ctx, path)
			if err != nil {
				fmt.Fprintf(execCtx.Stderr, "stat: %s: %v\n", path, err)
				return err
			}

			fmt.Fprintf(execCtx.Stdout, "  File: %s\n", stat.Key)
			fmt.Fprintf(execCtx.Stdout, "  Size: %d\n", stat.Size)
			fmt.Fprintf(execCtx.Stdout, "  Mode: %s\n", stat.Mode)
			fmt.Fprintf(execCtx.Stdout, "  Type: %s\n", getFileType(stat.Mode))
			if !stat.CreateTime.IsZero() {
				fmt.Fprintf(execCtx.Stdout, "Create: %s\n", stat.CreateTime.Format("2006-01-02 15:04:05"))
			}
			if !stat.ModifyTime.IsZero() {
				fmt.Fprintf(execCtx.Stdout, "Modify: %s\n", stat.ModifyTime.Format("2006-01-02 15:04:05"))
			}
			if !stat.AccessTime.IsZero() {
				fmt.Fprintf(execCtx.Stdout, "Access: %s\n", stat.AccessTime.Format("2006-01-02 15:04:05"))
			}
			if stat.UID != 0 || stat.GID != 0 {
				fmt.Fprintf(execCtx.Stdout, "   UID: %d\n", stat.UID)
				fmt.Fprintf(execCtx.Stdout, "   GID: %d\n", stat.GID)
			}
			if stat.ContentType != "" {
				fmt.Fprintf(execCtx.Stdout, "  Type: %s\n", stat.ContentType)
			}
			if stat.ETag != "" {
				fmt.Fprintf(execCtx.Stdout, "  ETag: %s\n", stat.ETag)
			}

			return nil
		},
	}
}

func helpCmd() *cmd.Command {
	return &cmd.Command{
		Use:   "help [command]",
		Short: "Display help for commands",
		Long:  "Display help information for the specified command or list all available commands",
		Args:  cmd.MaximumArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			execCtx := vfs.GetExecutionContext()

			if len(args) == 0 {
				// Show all commands
				fmt.Fprintf(execCtx.Stdout, "Available commands:\n\n")

				// Get root command
				root := c.Parent()
				if root == nil {
					fmt.Fprintf(execCtx.Stderr, "help: unable to get root command\n")
					return fmt.Errorf("unable to get root command")
				}

				children := root.GetAllChildren()
				for name, child := range children {
					fmt.Fprintf(execCtx.Stdout, "  %-12s %s\n", name, child.Short)
				}

				fmt.Fprintf(execCtx.Stdout, "\nUse 'help <command>' for more information about a specific command.\n")
			} else {
				// Show specific command help
				root := c.Parent()
				if root == nil {
					fmt.Fprintf(execCtx.Stderr, "help: unable to get root command\n")
					return fmt.Errorf("unable to get root command")
				}

				child := root.Children(args[0])
				if child == nil {
					fmt.Fprintf(execCtx.Stderr, "help: unknown command: %s\n", args[0])
					return fmt.Errorf("unknown command: %s", args[0])
				}

				fmt.Fprintf(execCtx.Stdout, "%s\n\n", child.Long)
				fmt.Fprintf(execCtx.Stdout, "Usage: %s\n", child.Use)

				// Show flags if any
				flags := child.Flags()
				if flags != nil {
					allFlags := flags.GetAllFlags()
					if len(allFlags) > 0 {
						fmt.Fprintf(execCtx.Stdout, "\nFlags:\n")
						for _, flag := range allFlags {
							shortFlag := ""
							if flag.Short != "" {
								shortFlag = fmt.Sprintf(", -%s", flag.Short)
							}
							fmt.Fprintf(execCtx.Stdout, "  --%s%s\n", flag.Name, shortFlag)
							fmt.Fprintf(execCtx.Stdout, "        %s\n", flag.Usage)
						}
					}
				}
			}

			return nil
		},
	}
}

// getFileType returns a human-readable file type
func getFileType(mode interface{ IsDir() bool; IsRegular() bool }) string {
	if mode.IsDir() {
		return "directory"
	}
	if mode.IsRegular() {
		return "regular file"
	}
	return "other"
}
