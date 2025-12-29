package cmd

import (
	"context"
	"io"
	"os"
	"sync"
)

// CommandContext holds long-lived state for command execution.
// This includes the current working directory, environment variables,
// and other session-level information.
type CommandContext struct {
	mu sync.RWMutex

	ctx              context.Context
	currentDirectory string
	environment      map[string]string
	aliases          map[string]string // Future: alias support
	history          []string          // Future: command history
}

// NewCommandContext creates a new command context with default values
func NewCommandContext() *CommandContext {
	return &CommandContext{
		ctx:              context.Background(),
		currentDirectory: "/",
		environment:      make(map[string]string),
		aliases:          make(map[string]string),
		history:          make([]string, 0, 100),
	}
}

// GetCurrentDirectory returns the current working directory
func (c *CommandContext) GetCurrentDirectory() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentDirectory
}

// SetCurrentDirectory sets the current working directory
func (c *CommandContext) SetCurrentDirectory(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentDirectory = path
}

// GetEnv retrieves an environment variable
func (c *CommandContext) GetEnv(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.environment[key]
	return val, ok
}

// SetEnv sets an environment variable
func (c *CommandContext) SetEnv(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.environment[key] = value
}

// DeleteEnv removes an environment variable
func (c *CommandContext) DeleteEnv(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.environment, key)
}

// GetAllEnv returns a copy of all environment variables
func (c *CommandContext) GetAllEnv() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	env := make(map[string]string, len(c.environment))
	for k, v := range c.environment {
		env[k] = v
	}
	return env
}

// GetAlias retrieves a command alias
func (c *CommandContext) GetAlias(name string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	alias, ok := c.aliases[name]
	return alias, ok
}

// SetAlias sets a command alias
func (c *CommandContext) SetAlias(name, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.aliases[name] = value
}

// DeleteAlias removes a command alias
func (c *CommandContext) DeleteAlias(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.aliases, name)
}

// AddHistory adds a command to the history
func (c *CommandContext) AddHistory(cmd string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.history = append(c.history, cmd)

	// Keep history limited to 100 entries
	if len(c.history) > 100 {
		c.history = c.history[1:]
	}
}

// GetHistory returns a copy of the command history
func (c *CommandContext) GetHistory() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	history := make([]string, len(c.history))
	copy(history, c.history)
	return history
}

// GetContext returns the context
func (c *CommandContext) GetContext() context.Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ctx
}

// SetContext sets the context
func (c *CommandContext) SetContext(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ctx = ctx
}

// ExecutionContext holds per-execution I/O streams.
// This is separate from CommandContext because it changes for each command execution
// (especially when using pipes and redirects).
type ExecutionContext struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewExecutionContext creates a new execution context with default streams
func NewExecutionContext() *ExecutionContext {
	return &ExecutionContext{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Clone creates a copy of the execution context
func (e *ExecutionContext) Clone() *ExecutionContext {
	return &ExecutionContext{
		Stdin:  e.Stdin,
		Stdout: e.Stdout,
		Stderr: e.Stderr,
	}
}
