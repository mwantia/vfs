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
		rmCmd(),
		cpCmd(),
		mvCmd(),

		// ls -alh [path]
		newListDirectoryCommand(),
		// mkdir [path]
		mkdirCmd(),
		// rmdir [path]
		rmdirCmd(),
		// cd [path]
		cdCmd(),
		pwdCmd(),
		walkCmd(),

		// Utility commands
		helpCmd(),
		echoCmd(),
		statCmd(),

		// Script execution
		sourceCmd(),
	}

	for _, c := range commands {
		if err := root.AddCommand(c); err != nil {
			return err
		}
	}

	return nil
}
