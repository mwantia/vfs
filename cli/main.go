package main

import (
	"context"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/backend/memory"
	"github.com/mwantia/vfs/backend/s3"
	"github.com/mwantia/vfs/backend/sqlite"
	"github.com/mwantia/vfs/command"
	"github.com/mwantia/vfs/command/builtin"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/log"
	"github.com/mwantia/vfs/mount"

	"github.com/mwantia/vfs/cli/tui"
)

// setupDemoVFS creates a demo filesystem with sample files and directories
func setupDemoVFS(ctx context.Context) (*vfs.VirtualFileSystem, error) {
	// Create VFS instance
	fs, err := vfs.NewVfs(vfs.WithLogLevel(log.Debug), vfs.WithLogFile("vfs.log"), vfs.WithoutTerminalLog())
	if err != nil {
		return nil, err
	}

	// Mount the backend at root
	if err := fs.Mount(ctx, "/", memory.NewMemoryBackend()); err != nil {
		return nil, fmt.Errorf("failed to mount: %w", err)
	}

	// Only mount S3 backends if credentials are provided via environment variables
	endpoint := os.Getenv("VFS_DEMO_ENDPOINT")
	accessKey := os.Getenv("VFS_DEMO_ACCESS_KEY")
	secretKey := os.Getenv("VFS_DEMO_SECRET_KEY")

	if endpoint != "" && accessKey != "" && secretKey != "" {
		gosyncMetadata, err := sqlite.NewSQLiteBackend("test/gosync.db")
		if err != nil {
			return nil, err
		}
		gosync, err := s3.NewS3Backend(endpoint, "gosync-storage", accessKey, secretKey, true)
		if err != nil {
			return nil, err
		}

		if err := fs.Mount(ctx, "/gosync", gosync, mount.AsReadOnly(), mount.WithMetadata(gosyncMetadata)); err != nil {
			return nil, fmt.Errorf("failed to mount: %w", err)
		}

		globalMetadata, err := sqlite.NewSQLiteBackend("test/global.db")
		if err != nil {
			return nil, err
		}
		global, err := s3.NewS3Backend(endpoint, "global-storage", accessKey, secretKey, true)
		if err != nil {
			return nil, err
		}

		if err := fs.Mount(ctx, "/global", global, mount.AsReadOnly(), mount.WithMetadata(globalMetadata)); err != nil {
			return nil, fmt.Errorf("failed to mount: %w", err)
		}
	}

	demoDirectories := []string{
		"/demo",
		"/demo/documents",
		"/demo/downloads",
		"/demo/logs",
		"/demo/config",
	}

	for _, dir := range demoDirectories {
		if err := fs.CreateDirectory(ctx, dir); err != nil {
			return nil, fmt.Errorf("failed to create directory '%s': %w", dir, err)
		}
	}

	demoFiles := map[string]string{
		"/demo/readme.txt":          "Welcome to the VFS demo!",
		"/demo/documents/notes.txt": "This is a sample document",
		"/demo/downloads/file1.dat": "Download One",
		"/demo/downloads/file2.dat": "Download Two",
		"/demo/config/config.conf":  "# Configuration file\nenabled = true",
		"/demo/logs/system.log":     "System log entry 1\nSystem log entry 2\nSystem log entry 3",
	}

	for path, content := range demoFiles {
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

	// Initialize debug logging
	if err := tui.InitDebugLog(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize debug log: %v\n", err)
		os.Exit(1)
	}
	defer tui.CloseDebugLog()

	tui.DebugLog("Starting VFS CLI")

	// Set up demo VFS
	fs, err := setupDemoVFS(ctx)
	if err != nil {
		tui.DebugLog("Failed to setup VFS: %v", err)
		fmt.Fprintf(os.Stderr, "Failed to setup VFS: %v\n", err)
		os.Exit(1)
	}
	tui.DebugLog("VFS initialized successfully")

	// Set up command center
	cmd := command.NewCommandCenter()
	if err := builtin.InitBuiltin(cmd); err != nil {
		tui.DebugLog("Failed to setup Command Center: %v", err)
		fmt.Fprintf(os.Stderr, "Failed to setup Command Center: %v\n", err)
		os.Exit(1)
	}
	tui.DebugLog("Command center initialized")

	// Create VFS adapter and TUI model
	adapter := tui.NewVFSAdapter(ctx, fs)
	model := tui.NewModel(adapter, cmd)

	// Start TUI
	tui.DebugLog("Starting TUI")
	p := tea.NewProgram(model, tea.WithAltScreen(), tea.WithMouseCellMotion())
	if _, err := p.Run(); err != nil {
		tui.DebugLog("TUI error: %v", err)
		fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
		os.Exit(1)
	}
	tui.DebugLog("TUI ended normally")

	// Clean up VFS mounts before exiting
	tui.DebugLog("Cleaning up VFS mounts")
	if err := fs.Close(ctx); err != nil {
		tui.DebugLog("Error closing VFS: %v", err)
		fmt.Fprintf(os.Stderr, "Warning: Failed to properly close VFS: %v\n", err)
	}
	tui.DebugLog("VFS cleanup complete")
}
