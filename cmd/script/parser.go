package script

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Parse reads a script file and returns a parsed Script
func Parse(reader io.Reader) (*Script, error) {
	scanner := bufio.NewScanner(reader)
	var lines []ScriptLine
	var lineNum int
	var continuation string
	var continuationStart int

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Trim whitespace
		line = strings.TrimSpace(line)

		// Skip blank lines
		if line == "" {
			continue
		}

		// Skip comments (only if not in continuation)
		if strings.HasPrefix(line, "#") && continuation == "" {
			continue
		}

		// Handle line continuation
		if strings.HasSuffix(line, "\\") {
			// Remove trailing backslash and accumulate
			line = strings.TrimSuffix(line, "\\")
			if continuation == "" {
				continuationStart = lineNum
			}
			continuation += line + " "
			continue
		}

		// Complete line (either standalone or end of continuation)
		if continuation != "" {
			line = continuation + line
			continuation = ""
			lines = append(lines, ScriptLine{
				Number:  continuationStart,
				Content: strings.TrimSpace(line),
			})
		} else {
			lines = append(lines, ScriptLine{
				Number:  lineNum,
				Content: line,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading script: %w", err)
	}

	// Check for incomplete continuation
	if continuation != "" {
		return nil, fmt.Errorf("incomplete line continuation at end of script (started at line %d)", continuationStart)
	}

	return &Script{Lines: lines}, nil
}
