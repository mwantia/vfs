package cmd

import (
	"fmt"
	"strconv"
	"strings"
)

func (s *FlagSet) String(name, short, defValue, usage string) {
	f := &Flag{
		Name:    strings.TrimSpace(name),
		Short:   strings.TrimSpace(short),
		Type:    "string",
		Default: defValue,
		Usage:   usage,
	}

	s.flags[f.Name] = f
}

func (s *FlagSet) GetString(flag string) (string, bool) {
	f, ok := s.flags[strings.TrimSpace(flag)]
	if ok {
		if f.Changed {
			v, _ := f.Value.(string)
			return v, true
		}

		v, _ := f.Default.(string)
		return v, true
	}

	return "", false
}

func (s *FlagSet) Bool(name, short string, defValue bool, usage string) {
	f := &Flag{
		Name:    strings.TrimSpace(name),
		Short:   strings.TrimSpace(short),
		Type:    "bool",
		Default: defValue,
		Usage:   usage,
	}

	s.flags[f.Name] = f
}

func (s *FlagSet) GetBool(flag string) (bool, bool) {
	f, ok := s.flags[strings.TrimSpace(flag)]
	if ok {
		if f.Changed {
			v, _ := f.Value.(bool)
			return v, true
		}

		v, _ := f.Default.(bool)
		return v, true
	}

	return false, false
}

func (s *FlagSet) Int(name, short string, defValue int, usage string) {
	f := &Flag{
		Name:    strings.TrimSpace(name),
		Short:   strings.TrimSpace(short),
		Type:    "int",
		Default: defValue,
		Usage:   usage,
	}

	s.flags[f.Name] = f
}

func (s *FlagSet) GetInt(flag string) (int, bool) {
	f, ok := s.flags[strings.TrimSpace(flag)]
	if ok {
		if f.Changed {
			v, _ := f.Value.(int)
			return v, true
		}

		v, _ := f.Default.(int)
		return v, true
	}

	return 0, false
}

// NewFlagSet creates a new flag set
func NewFlagSet() *FlagSet {
	return &FlagSet{
		flags: make(map[string]*Flag),
	}
}

// Parse parses flags from tokens and returns remaining positional arguments
func (s *FlagSet) Parse(tokens []Token) ([]string, error) {
	var positionalArgs []string
	i := 0

	for i < len(tokens) {
		token := tokens[i]

		// Only process word tokens for flag parsing
		if token.Type != TokenWord {
			positionalArgs = append(positionalArgs, token.Value)
			i++
			continue
		}

		word := token.Value

		// Not a flag - it's a positional argument
		if !strings.HasPrefix(word, "-") {
			positionalArgs = append(positionalArgs, word)
			i++
			continue
		}

		// Parse long flag (--flag)
		if strings.HasPrefix(word, "--") {
			consumed, err := s.parseLongFlag(tokens, i)
			if err != nil {
				return nil, err
			}
			i += consumed
			continue
		}

		// Parse short flag (-f)
		consumed, err := s.parseShortFlag(tokens, i)
		if err != nil {
			return nil, err
		}
		i += consumed
	}

	// Validate required flags
	if err := s.Validate(); err != nil {
		return nil, err
	}

	return positionalArgs, nil
}

// parseLongFlag parses a long flag (--flag or --flag=value)
func (s *FlagSet) parseLongFlag(tokens []Token, i int) (int, error) {
	word := tokens[i].Value
	name := strings.TrimPrefix(word, "--")

	// Check for --flag=value format
	if idx := strings.Index(name, "="); idx != -1 {
		flagName := name[:idx]
		flagValue := name[idx+1:]

		flag, ok := s.findFlag(flagName)
		if !ok {
			return 0, fmt.Errorf("unknown flag: --%s", flagName)
		}

		if err := s.parseValue(flag, flagValue); err != nil {
			return 0, fmt.Errorf("invalid value for flag --%s: %w", flagName, err)
		}

		return 1, nil
	}

	// Check if flag exists
	flag, ok := s.findFlag(name)
	if !ok {
		return 0, fmt.Errorf("unknown flag: --%s", name)
	}

	// Boolean flags don't need a value
	if flag.Type == "bool" {
		flag.Value = true
		flag.Changed = true
		return 1, nil
	}

	// Need next token for value
	if i+1 >= len(tokens) {
		return 0, fmt.Errorf("flag --%s requires a value", name)
	}

	nextToken := tokens[i+1]
	if nextToken.Type != TokenWord {
		return 0, fmt.Errorf("flag --%s requires a value", name)
	}

	if err := s.parseValue(flag, nextToken.Value); err != nil {
		return 0, fmt.Errorf("invalid value for flag --%s: %w", name, err)
	}

	return 2, nil
}

// parseShortFlag parses a short flag (-f or -f value or combined -abc)
func (s *FlagSet) parseShortFlag(tokens []Token, i int) (int, error) {
	word := tokens[i].Value
	name := strings.TrimPrefix(word, "-")

	// Check for -f=value format
	if idx := strings.Index(name, "="); idx != -1 {
		flagName := name[:idx]
		flagValue := name[idx+1:]

		flag, ok := s.findFlagByShort(flagName)
		if !ok {
			return 0, fmt.Errorf("unknown flag: -%s", flagName)
		}

		if err := s.parseValue(flag, flagValue); err != nil {
			return 0, fmt.Errorf("invalid value for flag -%s: %w", flagName, err)
		}

		return 1, nil
	}

	// Handle combined short flags (e.g., -alh)
	if len(name) > 1 {
		// Process each character as a separate flag
		for j, char := range name {
			charStr := string(char)
			flag, ok := s.findFlagByShort(charStr)
			if !ok {
				return 0, fmt.Errorf("unknown flag: -%s", charStr)
			}

			// All flags in a combined sequence must be boolean, except possibly the last
			if flag.Type != "bool" {
				// If it's not the last flag, it's an error
				if j < len(name)-1 {
					return 0, fmt.Errorf("non-boolean flag -%s cannot be combined with other flags", charStr)
				}

				// Last flag requires a value
				if i+1 >= len(tokens) {
					return 0, fmt.Errorf("flag -%s requires a value", charStr)
				}

				nextToken := tokens[i+1]
				if nextToken.Type != TokenWord {
					return 0, fmt.Errorf("flag -%s requires a value", charStr)
				}

				if err := s.parseValue(flag, nextToken.Value); err != nil {
					return 0, fmt.Errorf("invalid value for flag -%s: %w", charStr, err)
				}

				return 2, nil
			}

			// Set boolean flag
			flag.Value = true
			flag.Changed = true
		}

		return 1, nil
	}

	// Single short flag
	flag, ok := s.findFlagByShort(name)
	if !ok {
		return 0, fmt.Errorf("unknown flag: -%s", name)
	}

	// Boolean flags don't need a value
	if flag.Type == "bool" {
		flag.Value = true
		flag.Changed = true
		return 1, nil
	}

	// Need next token for value
	if i+1 >= len(tokens) {
		return 0, fmt.Errorf("flag -%s requires a value", name)
	}

	nextToken := tokens[i+1]
	if nextToken.Type != TokenWord {
		return 0, fmt.Errorf("flag -%s requires a value", name)
	}

	if err := s.parseValue(flag, nextToken.Value); err != nil {
		return 0, fmt.Errorf("invalid value for flag -%s: %w", name, err)
	}

	return 2, nil
}

// parseValue parses and sets the value for a flag
func (s *FlagSet) parseValue(flag *Flag, value string) error {
	switch flag.Type {
	case "string":
		flag.Value = value
		flag.Changed = true
		return nil

	case "int":
		intValue, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("expected integer, got %q", value)
		}
		flag.Value = intValue
		flag.Changed = true
		return nil

	case "bool":
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("expected boolean, got %q", value)
		}
		flag.Value = boolValue
		flag.Changed = true
		return nil

	default:
		return fmt.Errorf("unsupported flag type: %s", flag.Type)
	}
}

// findFlag finds a flag by its long name
func (s *FlagSet) findFlag(name string) (*Flag, bool) {
	flag, ok := s.flags[name]
	return flag, ok
}

// findFlagByShort finds a flag by its short name
func (s *FlagSet) findFlagByShort(short string) (*Flag, bool) {
	for _, flag := range s.flags {
		if flag.Short == short {
			return flag, true
		}
	}
	return nil, false
}

// Validate checks that all required flags have been set
func (s *FlagSet) Validate() error {
	for _, flag := range s.flags {
		if flag.Required && !flag.Changed {
			return fmt.Errorf("required flag missing: --%s", flag.Name)
		}
	}
	return nil
}

// GetAllFlags returns a copy of all flags
func (s *FlagSet) GetAllFlags() map[string]*Flag {
	result := make(map[string]*Flag, len(s.flags))
	for k, v := range s.flags {
		result[k] = v
	}
	return result
}
