package cmd

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser parses user-defined arguments into flags
type Parser struct {
	flagSet *CommandFlagSet
}

func NewParser(flagSet *CommandFlagSet) *Parser {
	return &Parser{
		flagSet: flagSet,
	}
}

func (cp *Parser) Parse(raw []string) (*CommandArgs, error) {
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
