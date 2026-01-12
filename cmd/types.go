package cmd

import (
	"io"
)

type Command struct {
	// Use is the one-line usage message.
	Use string

	// Short is the shortest description shown in help.
	Short string

	// Long is the longest description shown in help <command>
	Long string

	// Args defines validation for positional arguments.
	Args ArgsValidator

	Run func(vfs API, cmd *Command, args []string) error

	children map[string]*Command // Children
	parent   *Command            // Parent command (is set automatically when added as subcommand)
	flags    *FlagSet            // Flags for this command
	out      io.Writer           // Output writer for messages (set by VFS)
}

type Flag struct {
	Name  string // Long name (e.g., "output")
	Short string // Short name (e.g., "o")
	Usage string // Help text

	Required bool // Whether flag is required

	Type    string // "bool", "string", "int", "stringSlice"
	Changed bool
	Value   any
	Default any // Default value
}

type FlagSet struct {
	flags map[string]*Flag
}
