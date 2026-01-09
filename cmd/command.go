package cmd

import (
	"context"
	"fmt"
	"strings"
)

// Execute parses and runs the command with the given command string
func (c *Command) Execute(ctx context.Context, vfs API, cmdString string) (int, error) {
	// Tokenize command string
	tokenizer := NewTokenizer(cmdString)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return 1, fmt.Errorf("tokenization failed: %w", err)
	}

	// Check if command contains chaining operators
	hasChaining := containsChainingOperators(tokens)

	executor := NewExecutor(vfs, c)

	if hasChaining {
		// Parse as command chain
		chain, err := ParseCommandChain(tokens)
		if err != nil {
			return 1, fmt.Errorf("chain parse failed: %w", err)
		}
		return executor.ExecuteCommandChain(ctx, chain)
	}

	// Parse as single pipeline (existing behavior)
	pipeline, err := ParsePipeline(tokens)
	if err != nil {
		return 1, fmt.Errorf("pipeline parse failed: %w", err)
	}

	// Execute pipeline
	return executor.ExecutePipeline(ctx, pipeline)
}

// containsChainingOperators checks if tokens contain chaining operators
func containsChainingOperators(tokens []Token) bool {
	for _, t := range tokens {
		if t.Type == TokenSemicolon || t.Type == TokenAnd || t.Type == TokenOr {
			return true
		}
	}
	return false
}

// AddCommand adds a subcommand to this command
func (c *Command) AddCommand(sub *Command) error {
	if c.children == nil {
		c.children = make(map[string]*Command)
	}

	// Extract command name from Use field (format: "name [args]")
	parts := strings.Fields(sub.Use)
	if len(parts) == 0 {
		return fmt.Errorf("command Use field is empty")
	}
	name := parts[0]

	// Check for conflicts
	if _, exists := c.children[name]; exists {
		return fmt.Errorf("command %s already exists", name)
	}

	// Set parent and add to children
	sub.parent = c
	c.children[name] = sub

	// Initialize flag set if not present
	if sub.flags == nil {
		sub.flags = NewFlagSet()
	}

	return nil
}

// FindCommand resolves a command path from arguments
func (c *Command) FindCommand(args []string) (*Command, []string, error) {
	if len(args) == 0 {
		return c, args, nil
	}

	// Check if first arg is a subcommand
	if child, ok := c.children[args[0]]; ok {
		return child.FindCommand(args[1:])
	}

	// No subcommand found, this command should handle it
	return c, args, nil
}

// Flags returns the flag set for this command
func (c *Command) Flags() *FlagSet {
	if c.flags == nil {
		c.flags = NewFlagSet()
	}
	return c.flags
}

// Parent returns the parent command
func (c *Command) Parent() *Command {
	return c.parent
}

// HasChildren returns true if this command has subcommands
func (c *Command) HasChildren() bool {
	return len(c.children) > 0
}

// Children returns a specific child command by name
func (c *Command) Children(name string) *Command {
	if c.children == nil {
		return nil
	}
	return c.children[name]
}

// GetAllChildren returns all child commands
func (c *Command) GetAllChildren() map[string]*Command {
	if c.children == nil {
		return make(map[string]*Command)
	}

	// Return a copy to prevent external modification
	result := make(map[string]*Command, len(c.children))
	for k, v := range c.children {
		result[k] = v
	}
	return result
}
