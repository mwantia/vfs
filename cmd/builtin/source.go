package builtin

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/cmd/script"
	"github.com/mwantia/vfs/data"
)

func newSourceCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "source <file>",
		Short: "Execute commands from a script file",
		Long:  "Read and execute commands from a script file.\nBy default reads from VFS. Use -l/--local to read from host filesystem.\nOne command per line. Lines starting with # are comments.\nUse \\ at end of line for continuation.",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, cmd *cmd.Command, args []string) error {
			ctx := vfs.GetContext().GetContext()
			path := strings.TrimSpace(args[0])

			var reader io.ReadCloser
			var err error
			// Open file from either host filesystem or VFS
			if local, _ := cmd.Flags().GetBool("local"); local {
				// Open from host filesystem
				reader, err = os.Open(path)
				if err != nil {
					return fmt.Errorf("source: cannot open %s from host: %v", path, err)
				}
			} else {
				// Open from VFS
				reader, err = vfs.OpenFile(ctx, path, data.AccessModeRead)
				if err != nil {
					return fmt.Errorf("source: cannot open %s from VFS: %v", path, err)
				}
			}
			defer reader.Close()
			// Parse script
			parsed, err := script.Parse(reader)
			if err != nil {
				return fmt.Errorf("source: parse error in %s: %v", path, err)
			}

			continueOnError, _ := cmd.Flags().GetBool("continue")
			verbose, _ := cmd.Flags().GetBool("verbose")
			// Execute script
			opts := script.ScriptOptions{
				ContinueOnError: continueOnError,
				Verbose:         verbose,
			}
			// Get root command for script execution
			rootCmd := cmd
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

	cmd.Flags().Bool("local", "l", false, "Read script from host filesystem instead of VFS")
	cmd.Flags().Bool("continue", "c", false, "Continue execution even if commands fail")
	cmd.Flags().Bool("verbose", "v", false, "Print each command before executing")

	return cmd
}
