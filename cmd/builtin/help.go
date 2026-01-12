package builtin

import (
	"fmt"

	"github.com/mwantia/vfs/cmd"
)

func newHelpCommand() *cmd.Command {
	cmd := &cmd.Command{
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

	return cmd
}
