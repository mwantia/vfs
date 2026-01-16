package builtin

import (
	"github.com/mwantia/vfs/cmd"
)

// RegisterBuiltins registers all builtin commands with the root command
func RegisterBuiltins(root *cmd.Command) error {
	commands := []*cmd.Command{
		// mount <operation> -nprcmfvj [args]...
		newMountCommand(),
		// touch <path>
		newTouchCommand(),
		// cat [file]...
		newConcatenateCommand(),
		// tee [file]...
		newTeeCommand(),
		// stat -j <path>
		newStatCommand(),
		// etag -s <path>
		newETagCommand(),
		// mv <source> <destination>
		newMoveCommand(),
		// cp <source> <destination>
		newCopyCommand(),
		// help [command]
		newHelpCommand(),
		// printf [text]...
		newPrintfCommand(),
		// echo [text]...
		newEchoCommand(),
		// cd <path>
		newChangeDirectoryCommand(),
		// mkdir <path>
		newMakeDirectoryCommand(),
		// pwd
		newPrintWorkingDirectoryCommand(),
		// rm -rf <path>...
		newRemoveCommand(),
		// ls -alh [path]
		newListDirectoryCommand(),
		// source -cv <path>
		newSourceCommand(),
	}

	for _, c := range commands {
		if err := root.AddCommand(c); err != nil {
			return err
		}
	}

	return nil
}
