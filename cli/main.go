package main

import (
	"context"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/backend/memory"
	"github.com/mwantia/vfs/command"
	"github.com/mwantia/vfs/command/builtin"
	"github.com/mwantia/vfs/data"

	"github.com/mwantia/vfs/cli/tui"
)

// setupDemoVFS creates a demo filesystem with sample files and directories
func setupDemoVFS(ctx context.Context) (*vfs.VirtualFileSystem, error) {
	// Create VFS instance
	fs := vfs.NewVfs()

	// Mount the backend at root
	if err := fs.Mount(ctx, "/", memory.NewMemoryBackend()); err != nil {
		return nil, fmt.Errorf("failed to mount: %w", err)
	}

	// Create some demo directories
	dirs := []string{
		"/home",
		"/home/user",
		"/home/user/documents",
		"/home/user/downloads",
		"/etc",
		"/var",
		"/var/log",
		"/tmp",
	}

	for _, dir := range dirs {
		if err := fs.CreateDirectory(ctx, dir); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Create some demo files
	files := map[string]string{
		"/home/user/documents/readme.txt": "Welcome to the VFS demo!",
		"/home/user/documents/notes.txt":  "This is a sample file.",
		"/home/user/downloads/file1.dat":  "Download 1",
		"/home/user/downloads/file2.dat":  "Download 2",
		"/etc/config.conf":                "# Configuration file",
		"/var/log/system.log":             "System log entry 1\nSystem log entry 2",
		"/tmp/temp.txt":                   "Temporary file",
	}

	for path, content := range files {
		// Create and write to the file
		file, err := fs.OpenFile(ctx, path, data.AccessModeWrite|data.AccessModeCreate)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", path, err)
		}

		if _, err := file.Write([]byte(content)); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to write to file %s: %w", path, err)
		}

		file.Close()
	}

	return fs, nil
}

func main() {
	ctx := context.Background()

	// Set up demo VFS
	fs, err := setupDemoVFS(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup VFS: %v\n", err)
		os.Exit(1)
	}

	// Set up command center
	cmd := command.NewCommandCenter()
	if err := builtin.InitBuiltin(cmd); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup Command Center: %v\n", err)
		os.Exit(1)
	}

	// Create VFS adapter and TUI model
	adapter := tui.NewVFSAdapter(ctx, fs)
	model := tui.NewModel(adapter, cmd)

	// Start TUI
	p := tea.NewProgram(model, tea.WithAltScreen(), tea.WithMouseCellMotion())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
		os.Exit(1)
	}
}
