package vfs

import (
	"context"
	"fmt"
	"sync"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/cmd/builtin"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
	"github.com/mwantia/vfs/log"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/backend/memory"
)

// virtualFileSystemImpl is the main VFS manager that handles mount points and delegates
// file operations to the appropriate mount handlers. It provides a Unix-like filesystem
// abstraction with support for nested mounts and thread-safe operations.
type virtualFileSystemImpl struct {
	mu   sync.RWMutex
	log  *log.Logger
	cmds map[string]cmd.Command
	mnts map[string]*mount.Mount
}

// NewVfs creates a new VirtualFileSystem instance with no initial mounts.
func NewVirtualFileSystem(opts ...VirtualFileSystemOption) (VirtualFileSystem, error) {
	options := newDefaultVirtualFileSystemOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	vfs := &virtualFileSystemImpl{
		log:  log.NewLogger("vfs", options.LogLevel, options.LogFile, options.NoTerminalLog),
		cmds: make(map[string]cmd.Command),
		mnts: make(map[string]*mount.Mount),
	}

	vfs.log.Info("VFS initialized with log level: %s", options.LogLevel)

	// Create base root mount if enabled
	if options.BaseRootMount {
		vfs.log.Info("Creating base root mount at /")
		ctx := context.Background()
		root := memory.NewMemoryBackend()
		if err := vfs.Mount(ctx, "/", root, mount.AsReadOnly()); err != nil {
			vfs.log.Error("Failed to create base root mount: %v", err)
			return nil, err
		}
		vfs.log.Info("Base root mount created successfully")
	}

	if err := vfs.initBuiltinCommands(); err != nil {
		return nil, err
	}

	return vfs, nil
}

func (vfs *virtualFileSystemImpl) Populate(ctx context.Context) error {
	return fmt.Errorf("vfs: not implemented")
}

// Shutdown unmounts all mounted filesystems and releases all resources.
// This should be called when shutting down the VFS to ensure proper cleanup.
// Mounts are unmounted in reverse order (deepest first) to avoid dependency issues.
func (vfs *virtualFileSystemImpl) Shutdown(ctx context.Context) error {
	vfs.log.Info("Shutdown: shutting down VFS, unmounting all mounts")

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	// Collect all mount paths and sort them by depth (deepest first)
	var paths []string
	for path := range vfs.mnts {
		paths = append(paths, path)
	}

	vfs.log.Debug("Shutdown: found %d mount(s) to unmount", len(paths))

	// Sort by length (longer paths are deeper in the tree)
	for i := 0; i < len(paths); i++ {
		for j := i + 1; j < len(paths); j++ {
			if len(paths[j]) > len(paths[i]) {
				paths[i], paths[j] = paths[j], paths[i]
			}
		}
	}

	// Unmount all filesystems
	var lastErr error
	unmountedCount := 0
	failedCount := 0
	for _, path := range paths {
		if mnt, exists := vfs.mnts[path]; exists {
			vfs.log.Debug("Shutdown: unmounting %s", path)
			if err := mnt.Unmount(ctx, true); err != nil {
				vfs.log.Error("Close: failed to unmount %s - %v", path, err)
				lastErr = err
				failedCount++
				// Continue trying to unmount others even if one fails
			} else {
				vfs.log.Debug("Shutdown: successfully unmounted %s", path)
				unmountedCount++
			}
			delete(vfs.mnts, path)
		}
	}

	if failedCount > 0 {
		vfs.log.Warn("Shutdown: VFS closed with %d successful and %d failed unmounts", unmountedCount, failedCount)
	} else {
		vfs.log.Info("Shutdown: VFS closed successfully, unmounted %d mount(s)", unmountedCount)
	}

	return lastErr
}

// initBuiltinCommands
func (vfs *virtualFileSystemImpl) initBuiltinCommands() error {
	errs := errors.Errors{}

	errs.Add(vfs.RegisterCommand(&builtin.LsCommand{}))

	return errs.Errors()
}

// renameFile performs a copy-and-delete rename for a single file.
func (vfs *virtualFileSystemImpl) renameFile(ctx context.Context, oldPath string, newPath string, oldStat *data.VirtualFileMetadata) error {
	// Handle empty files specially
	if oldStat.Size == 0 {
		vfs.log.Debug("renameFile: creating empty file at %s", newPath)
		// Just create an empty file at the destination
		_, err := vfs.OpenFile(ctx, newPath, data.AccessModeCreate|data.AccessModeWrite)
		if err != nil {
			vfs.log.Error("renameFile: failed to create destination file %s - %v", newPath, err)
			return err
		}
		if err := vfs.CloseFile(ctx, newPath, false); err != nil {
			vfs.log.Error("renameFile: failed to close destination file %s - %v", newPath, err)
			return err
		}
	} else {
		// Read entire file content
		vfs.log.Debug("renameFile: reading %d bytes from %s", oldStat.Size, oldPath)
		contents, err := vfs.ReadFile(ctx, oldPath, 0, oldStat.Size)
		if err != nil {
			vfs.log.Error("renameFile: failed to read source file %s - %v", oldPath, err)
			return err
		}

		// Create destination file
		vfs.log.Debug("renameFile: creating destination file %s", newPath)
		_, err = vfs.OpenFile(ctx, newPath, data.AccessModeCreate|data.AccessModeWrite)
		if err != nil {
			vfs.log.Error("renameFile: failed to create destination file %s - %v", newPath, err)
			return err
		}

		// Write data to destination
		vfs.log.Debug("renameFile: writing %d bytes to %s", len(contents), newPath)
		n, err := vfs.WriteFile(ctx, newPath, 0, contents)
		if err != nil {
			vfs.log.Error("renameFile: failed to write to destination file %s - %v", newPath, err)
			// Clean up partial file
			vfs.CloseFile(ctx, newPath, true)
			vfs.UnlinkFile(ctx, newPath)
			return err
		}

		vfs.log.Debug("renameFile: wrote %d bytes to %s", n, newPath)

		// Close destination file
		if err := vfs.CloseFile(ctx, newPath, false); err != nil {
			vfs.log.Error("renameFile: failed to close destination file %s - %v", newPath, err)
			return err
		}
	}

	// Delete source file
	vfs.log.Debug("renameFile: deleting source file %s", oldPath)
	if err := vfs.UnlinkFile(ctx, oldPath); err != nil {
		vfs.log.Error("renameFile: failed to delete source file %s - %v", oldPath, err)
		// Note: destination file exists, but source couldn't be deleted - partial state
		return err
	}

	vfs.log.Info("renameFile: successfully renamed file %s to %s", oldPath, newPath)
	return nil
}

// renameDirectory performs a recursive copy-and-delete rename for a directory.
func (vfs *virtualFileSystemImpl) renameDirectory(ctx context.Context, oldPath string, newPath string) error {
	// Create the destination directory
	vfs.log.Debug("renameDirectory: creating destination directory %s", newPath)
	if err := vfs.CreateDirectory(ctx, newPath); err != nil {
		vfs.log.Error("renameDirectory: failed to create destination directory %s - %v", newPath, err)
		return err
	}

	// Read all entries in the source directory
	vfs.log.Debug("renameDirectory: reading entries from %s", oldPath)
	entries, err := vfs.ReadDirectory(ctx, oldPath)
	if err != nil {
		vfs.log.Error("renameDirectory: failed to read source directory %s - %v", oldPath, err)
		return err
	}

	vfs.log.Debug("renameDirectory: found %d entries in %s", len(entries), oldPath)

	// Recursively rename each entry
	for _, entry := range entries {
		// Skip mount points
		if entry.Mode&data.ModeMount != 0 {
			vfs.log.Debug("renameDirectory: skipping mount point %s", entry.Key)
			continue
		}

		oldEntryPath := oldPath + "/" + entry.Key
		newEntryPath := newPath + "/" + entry.Key

		vfs.log.Debug("renameDirectory: processing entry %s", entry.Key)

		// Recursively rename subdirectories and files
		if err := vfs.Rename(ctx, oldEntryPath, newEntryPath); err != nil {
			vfs.log.Error("renameDirectory: failed to rename entry %s - %v", entry.Key, err)
			return err
		}
	}

	// Remove the empty source directory
	vfs.log.Debug("renameDirectory: removing source directory %s", oldPath)
	if err := vfs.RemoveDirectory(ctx, oldPath, false); err != nil {
		vfs.log.Error("renameDirectory: failed to remove source directory %s - %v", oldPath, err)
		return err
	}

	vfs.log.Info("renameDirectory: successfully renamed directory %s to %s", oldPath, newPath)
	return nil
}
