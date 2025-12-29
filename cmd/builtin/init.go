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

		// Directory commands
		lsCmd(),
		mkdirCmd(),
		rmdirCmd(),
		cdCmd(),
		pwdCmd(),

		// Utility commands
		helpCmd(),
		echoCmd(),
		statCmd(),
	}

	for _, c := range commands {
		if err := root.AddCommand(c); err != nil {
			return err
		}
	}

	return nil
}
