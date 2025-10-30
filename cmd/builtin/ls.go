package builtin

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/data"
)

type LsCommand struct {
}

// Name returns the command identifier
func (ls *LsCommand) Name() string {
	return "ls"
}

// Description returns human-readable help text
func (ls *LsCommand) Description() string {
	return "List directory contents"
}

// Usage returns a usage string for help (e.g. "ls -al [path]")
func (ls *LsCommand) Usage() string {
	return "ls [OPTIONS] [PATH...]"
}

// Execute runs the command with parsed arguments
// Returns exit code (0 = success) and error message
func (ls *LsCommand) Execute(ctx context.Context, api cmd.API, args *cmd.CommandArgs, writer io.Writer) (int, error) {
	// Parse flags
	longFormat := getBoolFlag(args, "long")
	showAll := getBoolFlag(args, "all")
	humanReadable := getBoolFlag(args, "human-readable")
	recursive := getBoolFlag(args, "recursive")

	// Determine paths to list
	paths := args.Args
	if len(paths) == 0 {
		paths = []string{"/"}
	}

	// List each path
	for i, path := range paths {
		if err := ls.listPath(ctx, api, path, longFormat, showAll, humanReadable, recursive, 0, writer); err != nil {
			return 1, fmt.Errorf("cannot access '%s': %w", path, err)
		}

		// Add newline between multiple paths
		if len(paths) > 1 && i < len(paths)-1 {
			fmt.Fprintln(writer)
		}
	}

	return 0, nil
}

// listPath lists a single path with the given options
func (ls *LsCommand) listPath(ctx context.Context, api cmd.API, path string, longFormat, showAll, humanReadable, recursive bool, depth int, writer io.Writer) error {
	// Check if path exists
	exists, err := api.LookupMetadata(ctx, path)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("no such file or directory")
	}

	// Get metadata for the path
	metadata, err := api.StatMetadata(ctx, path)
	if err != nil {
		return err
	}

	// If it's a file, just list the file itself
	if !metadata.Mode.IsDir() {
		ls.printEntry(metadata, longFormat, humanReadable, writer)
		return nil
	}

	// Read directory contents
	entries, err := api.ReadDirectory(ctx, path)
	if err != nil {
		return err
	}

	// Filter hidden files if needed
	if !showAll {
		filtered := make([]*data.Metadata, 0, len(entries))
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Key, ".") {
				filtered = append(filtered, entry)
			}
		}
		entries = filtered
	}

	// Sort entries by name
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	// Print header for multiple paths or recursive mode
	if depth > 0 || recursive {
		fmt.Fprintf(writer, "%s:\n", path)
	}

	// Print entries
	for _, entry := range entries {
		ls.printEntry(entry, longFormat, humanReadable, writer)

		// Recursive listing
		if recursive && entry.Mode.IsDir() {
			fmt.Fprintln(writer)
			subPath := filepath.Join(path, entry.Key)
			if err := ls.listPath(ctx, api, subPath, longFormat, showAll, humanReadable, recursive, depth+1, writer); err != nil {
				fmt.Fprintf(writer, "cannot access '%s': %v\n", subPath, err)
			}
		}
	}

	return nil
}

// printEntry prints a single directory entry
func (ls *LsCommand) printEntry(metadata *data.Metadata, longFormat, humanReadable bool, writer io.Writer) {
	if longFormat {
		// Long format: permissions, size, time, name
		mode := metadata.Mode.String()
		size := formatSize(metadata.Size, humanReadable)
		modTime := metadata.ModifyTime.Format("Jan 02 15:04")
		name := metadata.Key
		if metadata.Mode.IsDir() {
			name += "/"
		} else if metadata.Mode.IsSymlink() {
			name += "@"
		}
		fmt.Fprintf(writer, "%s %8s %s %s\n", mode, size, modTime, name)
	} else {
		// Short format: just the name
		name := metadata.Key
		if metadata.Mode.IsDir() {
			name += "/"
		} else if metadata.Mode.IsSymlink() {
			name += "@"
		}
		fmt.Fprintln(writer, name)
	}
}

// formatSize formats a file size for display
func formatSize(size int64, humanReadable bool) string {
	if !humanReadable {
		return fmt.Sprintf("%d", size)
	}

	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%dB", size)
	}

	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"K", "M", "G", "T", "P", "E"}
	return fmt.Sprintf("%.1f%s", float64(size)/float64(div), units[exp])
}

// getBoolFlag safely retrieves a boolean flag value
func getBoolFlag(args *cmd.CommandArgs, name string) bool {
	if args.Flags == nil {
		return false
	}
	if val, ok := args.Flags[name]; ok {
		if boolVal, ok := val.(bool); ok {
			return boolVal
		}
	}
	return false
}

// GetFlags returns the flag set for this command
func (ls *LsCommand) GetFlags() *cmd.CommandFlagSet {
	return &cmd.CommandFlagSet{
		Flags: map[string]*cmd.CommandFlag{
			"long": {
				Name:        "long",
				Short:       "l",
				Type:        "bool",
				Default:     false,
				Description: "use a long listing format",
			},
			"all": {
				Name:        "all",
				Short:       "a",
				Type:        "bool",
				Default:     false,
				Description: "do not ignore entries starting with .",
			},
			"human-readable": {
				Name:        "human-readable",
				Short:       "h",
				Type:        "bool",
				Default:     false,
				Description: "print sizes in human readable format (e.g., 1K, 234M, 2G)",
			},
			"recursive": {
				Name:        "recursive",
				Short:       "R",
				Type:        "bool",
				Default:     false,
				Description: "list subdirectories recursively",
			},
		},
	}
}
