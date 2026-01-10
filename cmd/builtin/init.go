package builtin

import (
	"github.com/mwantia/vfs/cmd"
)

// RegisterBuiltins registers all builtin commands with the root command
func RegisterBuiltins(root *cmd.Command) error {
	commands := []*cmd.Command{
		// Mount commands
		mountCmd(),
		umountCmd(),

		// File commands
		catCmd(),
		teeCmd(),
		touchCmd(),
		cpCmd(),
		mvCmd(),

		// mkdir [path]
		mkdirCmd(),
		// cd [path]
		cdCmd(),
		walkCmd(),

		// Utility commands
		statCmd(),

		// help [command]
		newHelpCommand(),
		// printf [text]...
		newPrintfCommand(),
		// echo [text]...
		newEchoCommand(),
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
