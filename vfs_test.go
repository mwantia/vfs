package vfs_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/log"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/backend/direct"
	"github.com/mwantia/vfs/mount/backend/ephemeral"
	"github.com/mwantia/vfs/mount/backend/sqlite"
)

type TestMountFactory func(tst *testing.T, fs vfs.VirtualFileSystem) error

func GetTestMountFactories() map[string]TestMountFactory {
	return map[string]TestMountFactory{
		"ephemeral-only": func(tst *testing.T, fs vfs.VirtualFileSystem) error {
			ctx := tst.Context()
			storage := ephemeral.NewEphemeralBackend()

			return fs.Mount(ctx, "/", storage)
		},
		"sqlite-only": func(tst *testing.T, fs vfs.VirtualFileSystem) error {
			ctx := tst.Context()
			storage, err := sqlite.NewSQLiteBackend(":memory:")
			if err != nil {
				return err
			}

			return fs.Mount(ctx, "/", storage)
		},
		"direct-only": func(tst *testing.T, fs vfs.VirtualFileSystem) error {
			ctx := tst.Context()
			path := tst.TempDir()
			storage, err := direct.NewDirectBackend(path)
			if err != nil {
				return err
			}

			return fs.Mount(ctx, "/", storage)
		},

		"ephemeral-metadata": func(tst *testing.T, fs vfs.VirtualFileSystem) error {
			ctx := tst.Context()
			storage := ephemeral.NewEphemeralBackend()

			return fs.Mount(ctx, "/", storage, mount.WithMetadata(storage))
		},
		"sqlite-metadata": func(tst *testing.T, fs vfs.VirtualFileSystem) error {
			ctx := tst.Context()
			storage, err := sqlite.NewSQLiteBackend(":memory:")
			if err != nil {
				return err
			}
			metadata := ephemeral.NewEphemeralBackend()

			return fs.Mount(ctx, "/", storage, mount.WithMetadata(metadata))
		},
		"direct-metadata": func(tst *testing.T, fs vfs.VirtualFileSystem) error {
			ctx := tst.Context()
			path := tst.TempDir()
			storage, err := direct.NewDirectBackend(path)
			if err != nil {
				return err
			}

			metadata := ephemeral.NewEphemeralBackend()

			return fs.Mount(ctx, "/", storage, mount.WithMetadata(metadata))
		},
	}
}

// TestAllMounts_FileOperations verifies basic file create, write, and read operations across all backend implementations.
func TestAllMounts_FileOperations(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// TODO :: New Logic here...
			streamer, err := fs.OpenFile(ctx, "/test.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}

			buffer := []byte("hello world")
			if _, err := streamer.Write(buffer); err != nil {
				tst.Fatalf("Write failed: %v", err)
			}

			if err := streamer.Close(); err != nil {
				tst.Fatalf("Close failed: %v", err)
			}

			// Read file
			streamer, err = fs.OpenFile(ctx, "/test.txt", data.AccessModeRead)
			if err != nil {
				tst.Fatalf("Open for read failed: %v", err)
			}
			defer streamer.Close()

			got, err := io.ReadAll(streamer)
			if err != nil {
				tst.Fatalf("ReadAll failed: %v", err)
			}

			if !bytes.Equal(got, buffer) {
				tst.Errorf("Expected %q, got %q", buffer, got)
			}

			if err := streamer.Close(); err != nil {
				tst.Fatalf("Close failed: %v", err)
			}

			if err := fs.UnlinkFile(ctx, "/test.txt"); err != nil {
				tst.Fatalf("Unlink failed: %v", err)
			}

			if _, err := fs.StatMetadata(ctx, "/test.txt"); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist, got %v", err)
			}
		})
	}
}

// TestAllMounts_DirectoryOperations verifies directory creation, listing, and removal across all backend implementations.
func TestAllMounts_DirectoryOperations(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			if err := fs.CreateDirectory(ctx, "/data"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			for i, name := range []string{"file1.txt", "file2.txt", "file3.txt"} {
				f, err := fs.OpenFile(ctx, "/data/"+name, data.AccessModeWrite|data.AccessModeCreate)
				if err != nil {
					tst.Fatalf("Open %s failed: %v", name, err)
				}
				f.Write([]byte{byte(i)})
				f.Close()
			}

			entries, err := fs.ReadDirectory(ctx, "/data")
			if err != nil {
				tst.Fatalf("ReadDir failed: %v", err)
			}

			if len(entries) != 3 {
				tst.Errorf("Expected 3 entries, got %d", len(entries))
			}

			if err := fs.RemoveDirectory(ctx, "/data", false); err == nil {
				tst.Error("Expected error removing non-empty directory")
			}

			if _, err := fs.StatMetadata(ctx, "/data"); err != nil {
				tst.Errorf("Directory should still exist, got %v", err)
			}
		})
	}
}

// TestAllMounts_NestedPaths verifies deeply nested directory and file operations across all backend implementations.
func TestAllMounts_NestedPaths(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			if err := fs.CreateDirectory(ctx, "/a"); err != nil {
				tst.Fatalf("MkDir /a failed: %v", err)
			}
			if err := fs.CreateDirectory(ctx, "/a/b"); err != nil {
				tst.Fatalf("MkDir /a/b failed: %v", err)
			}
			if err := fs.CreateDirectory(ctx, "/a/b/c"); err != nil {
				tst.Fatalf("MkDir /a/b/c failed: %v", err)
			}

			streamer, err := fs.OpenFile(ctx, "/a/b/c/file.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open nested file failed: %v", err)
			}
			streamer.Write([]byte("nested"))
			streamer.Close()

			info, err := fs.StatMetadata(ctx, "/a/b/c/file.txt")
			if err != nil {
				tst.Fatalf("Stat nested file failed: %v", err)
			}

			if info.Mode.IsDir() {
				tst.Error("Expected file, got directory")
			}

			info, err = fs.StatMetadata(ctx, "/a/b")
			if err != nil {
				tst.Fatalf("Stat parent dir failed: %v", err)
			}

			if !info.Mode.IsDir() {
				tst.Error("Expected directory, got file")
			}
		})
	}
}

// TestAllMounts_ErrorCases verifies proper error handling for invalid operations across all backend implementations.
func TestAllMounts_ErrorCases(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			if _, err := fs.StatMetadata(ctx, "/nonexistent"); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist, got %v", err)
			}

			if _, err := fs.OpenFile(ctx, "/nonexistent", data.AccessModeRead); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist on Open for read, got %v", err)
			}

			if err := fs.CreateDirectory(ctx, "/testdir"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			if _, err := fs.OpenFile(ctx, "/testdir", data.AccessModeRead); err == nil {
				tst.Error("Expected error opening directory for reading")
			}

			if err := fs.UnlinkFile(ctx, "/testdir"); err == nil {
				tst.Error("Expected error unlinking directory")
			}
		})
	}
}

// TestAllMounts_StatOperations verifies file and directory stat operations across all backend implementations.
func TestAllMounts_StatOperations(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			streamer, err := fs.OpenFile(ctx, "/stattest.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}

			content := []byte("test content for stat")
			streamer.Write(content)
			streamer.Close()

			info, err := fs.StatMetadata(ctx, "/stattest.txt")
			if err != nil {
				tst.Fatalf("Stat failed: %v", err)
			}

			if info.Key != "stattest.txt" {
				tst.Errorf("Expected key 'stattest.txt', got %q", info.Key)
			}

			if info.Size != int64(len(content)) {
				tst.Errorf("Expected size %d, got %d", len(content), info.Size)
			}

			if info.Mode.IsDir() {
				tst.Error("Expected file, got directory")
			}

			if err := fs.CreateDirectory(ctx, "/statdir"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			dirInfo, err := fs.StatMetadata(ctx, "/statdir")
			if err != nil {
				tst.Fatalf("Stat directory failed: %v", err)
			}

			if !dirInfo.Mode.IsDir() {
				tst.Error("Expected directory, got file")
			}

			if dirInfo.Key != "statdir" {
				tst.Errorf("Expected key 'statdir', got %q", dirInfo.Key)
			}
		})
	}
}

// TestAllMounts_MultipleFilesOperations verifies handling of multiple concurrent files across all backend implementations.
func TestAllMounts_MultipleFilesOperations(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			fileCount := 10
			for i := 0; i < fileCount; i++ {
				filename := fmt.Sprintf("/file%d.txt", i)
				streamer, err := fs.OpenFile(ctx, filename, data.AccessModeWrite|data.AccessModeCreate)
				if err != nil {
					tst.Fatalf("Open %s failed: %v", filename, err)
				}

				content := []byte(fmt.Sprintf("content for file %d", i))
				streamer.Write(content)
				streamer.Close()
			}

			for i := 0; i < fileCount; i++ {
				filename := fmt.Sprintf("/file%d.txt", i)
				streamer, err := fs.OpenFile(ctx, filename, data.AccessModeRead)
				if err != nil {
					tst.Fatalf("Open %s for read failed: %v", filename, err)
				}

				got, _ := io.ReadAll(streamer)
				streamer.Close()

				expected := []byte(fmt.Sprintf("content for file %d", i))
				if !bytes.Equal(got, expected) {
					tst.Errorf("File %s: expected %q, got %q", filename, expected, got)
				}
			}
		})
	}
}

// TestAllMounts_FileAppendOperations verifies appending to existing files across all backend implementations
func TestAllMounts_FileAppendOperations(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			streamer, err := fs.OpenFile(ctx, "/append.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}
			streamer.Write([]byte("first "))
			streamer.Close()

			// Append to file
			streamer, err = fs.OpenFile(ctx, "/append.txt", data.AccessModeWrite|data.AccessModeAppend)
			if err != nil {
				tst.Fatalf("Open for append failed: %v", err)
			}
			streamer.Write([]byte("second"))
			streamer.Close()

			// Read and verify
			streamer, err = fs.OpenFile(ctx, "/append.txt", data.AccessModeRead)
			if err != nil {
				tst.Fatalf("Open for read failed: %v", err)
			}
			defer streamer.Close()

			got, _ := io.ReadAll(streamer)
			expected := []byte("first second")

			if !bytes.Equal(got, expected) {
				tst.Errorf("Expected %q, got %q", expected, got)
			}
		})
	}
}

// TestAllMounts_FileTruncateOperations verifies truncating existing files across all backend implementations.
func TestAllMounts_FileTruncateOperations(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			streamer, err := fs.OpenFile(ctx, "/trunc.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}
			streamer.Write([]byte("original content"))
			streamer.Close()

			streamer, err = fs.OpenFile(ctx, "/trunc.txt", data.AccessModeWrite|data.AccessModeTrunc)
			if err != nil {
				tst.Fatalf("Open for truncate failed: %v", err)
			}
			streamer.Write([]byte("new"))
			streamer.Close()

			streamer, err = fs.OpenFile(ctx, "/trunc.txt", data.AccessModeRead)
			if err != nil {
				tst.Fatalf("Open for read failed: %v", err)
			}
			defer streamer.Close()

			got, _ := io.ReadAll(streamer)
			expected := []byte("new")

			if !bytes.Equal(got, expected) {
				tst.Errorf("Expected %q, got %q", expected, got)
			}
		})
	}
}

// TestAllMounts_EmptyDirectoryOperations verifies empty directory operations across all backend implementations.
func TestAllMounts_EmptyDirectoryOperations(t *testing.T) {
	factories := GetTestMountFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, err := vfs.NewVirtualFileSystem(vfs.WithLogLevel(log.Debug))
			if err != nil {
				tst.Fatalf("Failed to initialize vfs: %v", err)
			}

			if err := factory(tst, fs); err != nil {
				tst.Fatalf("Failed to mount: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			if err := fs.CreateDirectory(ctx, "/empty"); err != nil {
				tst.Fatalf("Failed to create empty directory: %v", err)
			}

			entries, err := fs.ReadDirectory(ctx, "/empty")
			if err != nil {
				tst.Fatalf("Failed to read empty directory: %v", err)
			}

			if len(entries) != 0 {
				tst.Errorf("Expected 0 files in directory, got %d instead", len(entries))
			}

			if err := fs.RemoveDirectory(ctx, "/empty", false); err != nil {
				tst.Fatalf("Failed to delete empty directory: %v", err)
			}

			if _, err := fs.StatMetadata(ctx, "/empty"); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist after deletion, but got '%v'", err)
			}

			if err := fs.Unmount(ctx, "/", false); err != nil {
				tst.Fatalf("failed to unmount primary backend: %v", err)
			}
		})
	}
}
