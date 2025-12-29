package cmd

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// Executor executes command pipelines
type Executor struct {
	vfs     API
	rootCmd *Command
}

// NewExecutor creates a new executor
func NewExecutor(vfs API, rootCmd *Command) *Executor {
	return &Executor{
		vfs:     vfs,
		rootCmd: rootCmd,
	}
}

// ExecutePipeline executes a multi-stage pipeline with io.Pipe connections
func (e *Executor) ExecutePipeline(ctx context.Context, pipeline *Pipeline) (int, error) {
	if len(pipeline.Stages) == 0 {
		return 1, fmt.Errorf("empty pipeline")
	}

	// Single command (no pipes)
	if len(pipeline.Stages) == 1 {
		execCtx := NewExecutionContext()
		return e.executeStage(ctx, pipeline.Stages[0], execCtx)
	}

	// Multi-stage pipeline with io.Pipe connections
	return e.executePipelineStages(ctx, pipeline.Stages)
}

// executePipelineStages executes multiple stages connected by pipes
func (e *Executor) executePipelineStages(ctx context.Context, stages []*PipelineStage) (int, error) {
	var wg sync.WaitGroup
	errors := make([]error, len(stages))
	exitCodes := make([]int, len(stages))

	// Create pipes between stages
	pipes := make([]*io.PipeWriter, len(stages)-1)
	pipeReaders := make([]*io.PipeReader, len(stages)-1)

	for i := 0; i < len(stages)-1; i++ {
		pr, pw := io.Pipe()
		pipeReaders[i] = pr
		pipes[i] = pw
	}

	// Execute all stages concurrently
	for i, stage := range stages {
		wg.Add(1)

		// Set up execution context for this stage
		execCtx := NewExecutionContext()

		// First stage: stdin from default (os.Stdin) or context
		// Middle stages: stdin from previous pipe
		// Last stage: stdin from previous pipe
		if i > 0 {
			execCtx.Stdin = pipeReaders[i-1]
		}

		// First/middle stages: stdout to next pipe
		// Last stage: stdout to default (os.Stdout) or context
		if i < len(stages)-1 {
			execCtx.Stdout = pipes[i]
		}

		// Launch stage in goroutine
		go func(idx int, stg *PipelineStage, ectx *ExecutionContext) {
			defer wg.Done()

			// Execute stage
			exitCodes[idx], errors[idx] = e.executeStage(ctx, stg, ectx)

			// Close pipe writer when done (if this stage writes to a pipe)
			if idx < len(stages)-1 {
				pipes[idx].Close()
			}

			// Close pipe reader when done (if this stage reads from a pipe)
			if idx > 0 {
				pipeReaders[idx-1].Close()
			}
		}(i, stage, execCtx)
	}

	// Wait for all stages to complete
	wg.Wait()

	// Return exit code of last stage (standard shell behavior)
	lastIdx := len(exitCodes) - 1
	return exitCodes[lastIdx], errors[lastIdx]
}

// executeStage executes a single command stage with redirects
func (e *Executor) executeStage(ctx context.Context, stage *PipelineStage, execCtx *ExecutionContext) (int, error) {
	// Apply redirects (modifies execCtx)
	cleanup, err := e.applyRedirects(ctx, stage.Redirects, execCtx)
	if err != nil {
		return 1, fmt.Errorf("failed to apply redirects: %w", err)
	}
	defer cleanup()

	// Build args slice for FindCommand
	args := append([]string{stage.Command.Name}, stage.Command.Args...)

	// Find command
	cmd, remainingArgs, err := e.rootCmd.FindCommand(args)
	if err != nil {
		return 1, fmt.Errorf("command not found: %w", err)
	}

	// Convert args to tokens for flag parsing
	argTokens := make([]Token, len(remainingArgs))
	for i, arg := range remainingArgs {
		argTokens[i] = Token{Type: TokenWord, Value: arg}
	}

	// Parse flags
	positionalArgs, err := cmd.Flags().Parse(argTokens)
	if err != nil {
		return 1, fmt.Errorf("flag parsing failed: %w", err)
	}

	// Validate arguments
	if cmd.Args != nil {
		if err := cmd.Args.Validate(positionalArgs); err != nil {
			return 1, fmt.Errorf("argument validation failed: %w", err)
		}
	}

	// Execute command Run function
	if cmd.Run == nil {
		return 1, fmt.Errorf("command %s has no Run function", stage.Command.Name)
	}

	// Store execution context in command for access during Run
	cmd.out = execCtx.Stdout

	// Temporarily set execution context in VFS for the duration of this command
	// This allows commands to access stdin/stdout/stderr via GetExecutionContext()
	e.vfs.SetExecutionContext(execCtx)

	if err := cmd.Run(e.vfs, cmd, positionalArgs); err != nil {
		return 1, err
	}

	return 0, nil
}
