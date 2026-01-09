package builtin

import (
	"fmt"
	"os"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/cmd/script"
)

func sourceCmd() *cmd.Command {
	c := &cmd.Command{
		Use:   "source <file>",
		Short: "Execute commands from a script file",
		Long:  "Read and execute commands from a script file on the host filesystem.\nOne command per line. Lines starting with # are comments.\nUse \\ at end of line for continuation.",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			execCtx := vfs.GetExecutionContext()

			scriptPath := args[0]

			// Get flags
			continueOnError, _ := c.Flags().GetBool("continue")
			verbose, _ := c.Flags().GetBool("verbose")

			// Open file from HOST filesystem (not VFS)
			file, err := os.Open(scriptPath)
			if err != nil {
				fmt.Fprintf(execCtx.Stderr, "source: cannot open %s: %v\n", scriptPath, err)
				return err
			}
			defer file.Close()

			// Parse script
			parsed, err := script.Parse(file)
			if err != nil {
				fmt.Fprintf(execCtx.Stderr, "source: parse error in %s: %v\n", scriptPath, err)
				return err
			}

			// Execute script
			opts := script.ScriptOptions{
				ContinueOnError: continueOnError,
				Verbose:         verbose,
			}

			// Get root command for script execution
			rootCmd := c
			for rootCmd.Parent() != nil {
				rootCmd = rootCmd.Parent()
			}

			exitCode, err := script.ExecuteScript(ctx, vfs, rootCmd, parsed, opts)
			if err != nil {
				return fmt.Errorf("script execution failed with exit code %d", exitCode)
			}

			return nil
		},
	}

	// Add flags
	c.Flags().Bool("continue", "", false, "Continue execution even if commands fail")
	c.Flags().Bool("verbose", "v", false, "Print each command before executing")

	return c
}
