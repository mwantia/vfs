package vfs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Command represents an executable command within the virtual filesystem.
type Command interface {
	// Name returns the command identifier
	Name() string

	// Description returns human-readable help text
	Description() string

	// Usage returns a usage string for help (e.g. "ls -al [path]")
	Usage() string

	// Execute runs the command with parsed arguments
	// Returns exit code (0 = success) and error message
	Execute(ctx context.Context, vfs *VirtualFileSystem, args *CommandArgs) (int, error)

	// GetFlags returns the flag set for this command (this is optional)
	GetFlags() *CommandFlagSet
}

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

// CommandParser parses user-defined arguments into flags
type CommandParser struct {
	flagSet *CommandFlagSet
}

// CommandManager handles command registration, parsing, and execution
type CommandManager struct {
	mu   sync.RWMutex
	vfs  *VirtualFileSystem
	cmds map[string]Command
}

// Register registers a custom command
func (cm *CommandManager) Register(cmd Command) error {
	if cmd == nil {
		return fmt.Errorf("command cannot be nil")
	}

	name := cmd.Name()
	if name == "" {
		return fmt.Errorf("command name cannot be empty")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.cmds[name]; exists {
		return fmt.Errorf("command already registered: %s", name)
	}

	cm.cmds[name] = cmd
	return nil
}

// Unregister removes a registered command
func (cm *CommandManager) Unregister(name string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.cmds[name]; !exists {
		return fmt.Errorf("command not found: %s", name)
	}

	delete(cm.cmds, name)
	return nil
}

// Get returns a command by name
func (cm *CommandManager) Get(name string) (Command, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	cmd, exists := cm.cmds[name]
	if !exists {
		return nil, fmt.Errorf("command not found: %s", name)
	}

	return cmd, nil
}

// ListAll returns all registered commands
func (cm *CommandManager) List() []Command {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	commands := make([]Command, 0, len(cm.cmds))
	for _, cmd := range cm.cmds {
		commands = append(commands, cmd)
	}

	return commands
}

// Execute parses and executes a command
func (cm *CommandManager) Execute(ctx context.Context, args ...string) (int, error) {
	if len(args) == 0 {
		return 1, fmt.Errorf("no command specified")
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	cmdName := args[0]
	cmdArgs := args[1:]

	cmd, err := cm.Get(cmdName)
	if err != nil {
		return 1, err
	}

	flagSet := cmd.GetFlags()
	if flagSet == nil {
		flagSet = &CommandFlagSet{Flags: make(map[string]*CommandFlag)}
	}

	parser := &CommandParser{
		flagSet: flagSet,
	}
	parsedArgs, err := parser.Parse(cmdArgs)
	if err != nil {
		return 1, fmt.Errorf("parse error: %w", err)
	}

	return cmd.Execute(ctx, cm.vfs, parsedArgs)
}

func (cp *CommandParser) Parse(raw []string) (*CommandArgs, error) {
	args := &CommandArgs{
		Flags: make(map[string]any),
		Raw:   raw,
	}

	for flagName, flag := range cp.flagSet.Flags {
		if flag.Default != nil {
			args.Flags[flagName] = flag.Default
		}
	}

	longToName := make(map[string]string)
	shortToName := make(map[string]string)
	for flagName, flag := range cp.flagSet.Flags {
		longToName[flag.Name] = flagName
		if flag.Short != "" {
			shortToName[flag.Short] = flagName
		}
	}

	for i := 0; i < len(raw); i++ {
		arg := raw[i]

		if arg == "--" {
			args.Args = append(args.Args, raw[i+1:]...)
			break
		}

		if strings.HasPrefix(arg, "--") {
			key, value, hasValue := parseLongFlag(arg)
			flagName, exists := longToName[key]
			if !exists {
				return nil, fmt.Errorf("unknown flag: --%s", key)
			}

			flag := cp.flagSet.Flags[flagName]
			if flag.Type == "bool" {
				args.Flags[flagName] = true
			} else if hasValue {
				args.Flags[flagName] = coerce(value, flag.Type)
			} else if i+1 < len(raw) && !strings.HasPrefix(raw[i+1], "-") {
				args.Flags[flagName] = coerce(raw[i+1], flag.Type)
				i++
			} else {
				return nil, fmt.Errorf("flag %s requires a value", key)
			}
			continue
		}

		if strings.HasPrefix(arg, "-") && len(arg) > 1 && arg != "-" {
			shortFlags := arg[1:]

			for j, shortChar := range shortFlags {
				shortStr := string(shortChar)
				flagName, exists := shortToName[shortStr]
				if !exists {
					return nil, fmt.Errorf("unknown flag: -%s", shortStr)
				}

				flag := cp.flagSet.Flags[flagName]

				if flag.Type == "bool" {
					args.Flags[flagName] = true
				} else {
					var value string
					if j+1 < len(shortFlags) {
						value = shortFlags[j+1:]
						args.Flags[flagName] = coerce(value, flag.Type)
						break
					} else if i+1 < len(raw) && !strings.HasPrefix(raw[i+1], "-") {
						value = raw[i+1]
						args.Flags[flagName] = coerce(value, flag.Type)
						i++
					} else {
						return nil, fmt.Errorf("flag -%s requires a value", shortStr)
					}
				}
			}
			continue
		}

		args.Args = append(args.Args, arg)
	}

	for flagName, flag := range cp.flagSet.Flags {
		if flag.Required {
			if _, ok := args.Flags[flagName]; !ok {
				if flag.Short != "" {
					return nil, fmt.Errorf("required flag: -%s / --%s", flag.Short, flag.Name)
				} else {
					return nil, fmt.Errorf("required flag: --%s", flag.Name)
				}
			}
		}
	}

	return args, nil
}

func parseLongFlag(arg string) (key, value string, hasValue bool) {
	arg = strings.TrimPrefix(arg, "--")
	if idx := strings.Index(arg, "="); idx >= 0 {
		return arg[:idx], arg[idx+1:], true
	}
	return arg, "", false
}

func coerce(value string, typeStr string) interface{} {
	switch typeStr {
	case "string":
		return value
	case "int":
		v, _ := strconv.ParseInt(value, 10, 64)
		return v
	case "bool":
		return value == "true" || value == "1" || value == "yes"
	default:
		return value
	}
}
