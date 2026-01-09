package script

import (
	"context"
	"fmt"

	"github.com/mwantia/vfs/cmd"
)

// ExecuteScript executes a parsed script
func ExecuteScript(ctx context.Context, vfs cmd.API, rootCmd *cmd.Command, script *Script, opts ScriptOptions) (int, error) {
	var firstError error
	var lastExitCode int

	execCtx := vfs.GetExecutionContext()

	for _, line := range script.Lines {
		// Print command if verbose
		if opts.Verbose {
			fmt.Fprintf(execCtx.Stderr, "[line %d] %s\n", line.Number, line.Content)
		}

		// Execute command through VFS Execute
		exitCode, err := rootCmd.Execute(ctx, vfs, line.Content)
		lastExitCode = exitCode

		// Handle errors
		if err != nil {
			// Print error with line number
			fmt.Fprintf(execCtx.Stderr, "Error at line %d: %v\n", line.Number, err)

			// Record first error
			if firstError == nil {
				firstError = err
			}

			// Stop if not in continue mode
			if !opts.ContinueOnError {
				return exitCode, fmt.Errorf("script failed at line %d: %w", line.Number, err)
			}
		}
	}

	// In continue mode, return last exit code
	if opts.ContinueOnError && firstError != nil {
		return lastExitCode, fmt.Errorf("script completed with errors (first error: %w)", firstError)
	}

	return lastExitCode, nil
}
