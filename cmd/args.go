package cmd

// CommandArgs contains parsed command arguments
type CommandArgs struct {
	// Positional arguments (command-specific)
	Args []string

	// Parsed flags
	Flags map[string]any

	// Raw unparsed arguments (for custom parsing)
	Raw []string
}

// CommandFlagSet defines the expected flags for a command
type CommandFlagSet struct {
	Flags map[string]*CommandFlag
}

// CommandFlag represents a single command-line flag
type CommandFlag struct {
	Name        string `json:"name"`              // e.g., "type" or "t"
	Short       string `json:"short"`             // Single-char shorthand (e.g., "t")
	Type        string `json:"type"`              // "string", "bool", "int", "stringSlice"
	Default     any    `json:"default,omitempty"` // Default value
	Required    bool   `json:"required"`          // Must be provided
	Description string `json:"description"`       // Help text
	Multiple    bool   `json:"multiple"`          // Can be specified multiple times
}
