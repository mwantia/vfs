package cmd

import "fmt"

// Pipeline represents a multi-stage command pipeline
type Pipeline struct {
	Stages []*PipelineStage
}

// PipelineStage represents a single stage in a pipeline
type PipelineStage struct {
	Command   *ParsedCommand
	Redirects []*Redirect
}

// ParsedCommand represents a command name and its arguments
type ParsedCommand struct {
	Name string
	Args []string
}

// Redirect represents a file redirection
type Redirect struct {
	Type RedirectType
	File string
}

// RedirectType specifies the type of redirection
type RedirectType int

const (
	RedirectStdinFrom      RedirectType = iota // < file
	RedirectStdoutTo                           // > file
	RedirectStdoutAppend                       // >> file
	RedirectStderrTo                           // 2> file
	RedirectStderrToStdout                     // 2>&1
)

// String returns a string representation of the redirect type
func (r RedirectType) String() string {
	switch r {
	case RedirectStdinFrom:
		return "StdinFrom"
	case RedirectStdoutTo:
		return "StdoutTo"
	case RedirectStdoutAppend:
		return "StdoutAppend"
	case RedirectStderrTo:
		return "StderrTo"
	case RedirectStderrToStdout:
		return "StderrToStdout"
	default:
		return "Unknown"
	}
}

// ParsePipeline parses tokens into a pipeline structure
func ParsePipeline(tokens []Token) (*Pipeline, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	// Split tokens by pipe operators
	stageTokens := splitByPipes(tokens)

	pipeline := &Pipeline{
		Stages: make([]*PipelineStage, 0, len(stageTokens)),
	}

	// Parse each stage
	for i, tokens := range stageTokens {
		stage, err := parseStage(tokens)
		if err != nil {
			return nil, fmt.Errorf("stage %d: %w", i+1, err)
		}
		pipeline.Stages = append(pipeline.Stages, stage)
	}

	return pipeline, nil
}

// splitByPipes splits tokens into stages separated by pipe operators
func splitByPipes(tokens []Token) [][]Token {
	var stages [][]Token
	var current []Token

	for _, token := range tokens {
		if token.Type == TokenPipe {
			if len(current) > 0 {
				stages = append(stages, current)
				current = nil
			}
			continue
		}
		current = append(current, token)
	}

	// Add final stage
	if len(current) > 0 {
		stages = append(stages, current)
	}

	return stages
}

// parseStage parses a single pipeline stage
func parseStage(tokens []Token) (*PipelineStage, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty stage")
	}

	// Extract redirects from tokens
	redirects, commandTokens, err := parseRedirects(tokens)
	if err != nil {
		return nil, err
	}

	// Build command from remaining tokens
	cmd, err := parseCommand(commandTokens)
	if err != nil {
		return nil, err
	}

	return &PipelineStage{
		Command:   cmd,
		Redirects: redirects,
	}, nil
}

// parseRedirects extracts redirect operators and returns redirects + remaining tokens
func parseRedirects(tokens []Token) ([]*Redirect, []Token, error) {
	var redirects []*Redirect
	var commandTokens []Token

	i := 0
	for i < len(tokens) {
		token := tokens[i]

		// Check for redirect operators
		var redirectType RedirectType
		var hasRedirect bool

		switch token.Type {
		case TokenRedirectIn:
			redirectType = RedirectStdinFrom
			hasRedirect = true
		case TokenRedirectOut:
			redirectType = RedirectStdoutTo
			hasRedirect = true
		case TokenRedirectAppend:
			redirectType = RedirectStdoutAppend
			hasRedirect = true
		case TokenRedirectErr:
			redirectType = RedirectStderrTo
			hasRedirect = true
		case TokenRedirectErrOut:
			redirectType = RedirectStderrToStdout
			hasRedirect = true
			// 2>&1 doesn't need a file
			redirects = append(redirects, &Redirect{
				Type: redirectType,
				File: "",
			})
			i++
			continue
		}

		if hasRedirect {
			// Need next token for filename (except 2>&1)
			if i+1 >= len(tokens) {
				return nil, nil, fmt.Errorf("redirect operator %s requires a filename", token.Type)
			}

			nextToken := tokens[i+1]
			if nextToken.Type != TokenWord {
				return nil, nil, fmt.Errorf("redirect operator %s requires a filename, got %s", token.Type, nextToken.Type)
			}

			redirects = append(redirects, &Redirect{
				Type: redirectType,
				File: nextToken.Value,
			})

			i += 2
			continue
		}

		// Not a redirect, add to command tokens
		commandTokens = append(commandTokens, token)
		i++
	}

	// Validate redirects
	if err := validateRedirects(redirects); err != nil {
		return nil, nil, err
	}

	return redirects, commandTokens, nil
}

// parseCommand parses command tokens into a ParsedCommand
func parseCommand(tokens []Token) (*ParsedCommand, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("no command specified")
	}

	// First token must be a word (command name)
	if tokens[0].Type != TokenWord {
		return nil, fmt.Errorf("expected command name, got %s", tokens[0].Type)
	}

	cmd := &ParsedCommand{
		Name: tokens[0].Value,
		Args: make([]string, 0, len(tokens)-1),
	}

	// Remaining tokens are arguments
	for i := 1; i < len(tokens); i++ {
		if tokens[i].Type != TokenWord {
			return nil, fmt.Errorf("unexpected token %s in command arguments", tokens[i].Type)
		}
		cmd.Args = append(cmd.Args, tokens[i].Value)
	}

	return cmd, nil
}

// validateRedirects checks for conflicting redirects
func validateRedirects(redirects []*Redirect) error {
	stdoutCount := 0
	stderrCount := 0
	stdinCount := 0

	for _, r := range redirects {
		switch r.Type {
		case RedirectStdoutTo, RedirectStdoutAppend:
			stdoutCount++
		case RedirectStderrTo, RedirectStderrToStdout:
			stderrCount++
		case RedirectStdinFrom:
			stdinCount++
		}
	}

	if stdoutCount > 1 {
		return fmt.Errorf("multiple stdout redirects specified")
	}
	if stderrCount > 1 {
		return fmt.Errorf("multiple stderr redirects specified")
	}
	if stdinCount > 1 {
		return fmt.Errorf("multiple stdin redirects specified")
	}

	return nil
}
