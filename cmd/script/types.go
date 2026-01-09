package script

// Script represents a parsed script
type Script struct {
	Lines []ScriptLine
}

// ScriptLine represents a single logical line in the script
type ScriptLine struct {
	Number  int    // Original line number (for error reporting)
	Content string // Command text (after joining continuations)
}

// ScriptOptions configures script execution
type ScriptOptions struct {
	ContinueOnError bool // If true, continue executing after errors
	Verbose         bool // If true, print each command before executing
}
