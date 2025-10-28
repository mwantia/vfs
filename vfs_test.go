package vfs_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/backend"
	"github.com/mwantia/vfs/mount/backend/local"
	"github.com/mwantia/vfs/mount/backend/memory"
	"github.com/mwantia/vfs/mount/backend/sqlite"
)

// TestBackendFactory creates a new backend instance for testing.
type TestBackendFactory func(t *testing.T) (backend.VirtualObjectStorageBackend, error)

// GetTestBackendFactories returns all backend implementations to test.
func GetTestBackendFactories() map[string]TestBackendFactory {
	return map[string]TestBackendFactory{
		"memory": func(t *testing.T) (backend.VirtualObjectStorageBackend, error) {
			return memory.NewMemoryBackend(), nil
		},
		"sqlite": func(t *testing.T) (backend.VirtualObjectStorageBackend, error) {
			return sqlite.NewSQLiteBackend(":memory:")
		},
		"local": func(t *testing.T) (backend.VirtualObjectStorageBackend, error) {
			return local.NewLocalBackend(t.TempDir()), nil
		},
	}
}

// TestAllBackends_FileOperations verifies basic file create, write, and read operations
// across all backend implementations.
func TestAllBackends_FileOperations(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// Create and write file
			f, err := fs.OpenFile(ctx, "/test.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}

			buffer := []byte("hello world")
			if _, err := f.Write(buffer); err != nil {
				tst.Fatalf("Write failed: %v", err)
			}

			if err := f.Close(); err != nil {
				tst.Fatalf("Close failed: %v", err)
			}

			// Read file
			f, err = fs.OpenFile(ctx, "/test.txt", data.AccessModeRead)
			if err != nil {
				tst.Fatalf("Open for read failed: %v", err)
			}
			defer f.Close()

			got, err := io.ReadAll(f)
			if err != nil {
				tst.Fatalf("ReadAll failed: %v", err)
			}

			if !bytes.Equal(got, buffer) {
				tst.Errorf("Expected %q, got %q", buffer, got)
			}

			// Close before unlink
			f.Close()

			// Remove file
			if err := fs.UnlinkFile(ctx, "/test.txt"); err != nil {
				tst.Fatalf("Unlink failed: %v", err)
			}

			// Verify removed
			if _, err := fs.StatMetadata(ctx, "/test.txt"); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist, got %v", err)
			}
		})
	}
}

// TestAllBackends_DirectoryOperations verifies directory creation, listing, and removal
// across all backend implementations.
func TestAllBackends_DirectoryOperations(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// Create directory
			if err := fs.CreateDirectory(ctx, "/data"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			// Create files in directory
			for i, name := range []string{"file1.txt", "file2.txt", "file3.txt"} {
				f, err := fs.OpenFile(ctx, "/data/"+name, data.AccessModeWrite|data.AccessModeCreate)
				if err != nil {
					tst.Fatalf("Open %s failed: %v", name, err)
				}
				f.Write([]byte{byte(i)})
				f.Close()
			}

			// List directory
			entries, err := fs.ReadDirectory(ctx, "/data")
			if err != nil {
				tst.Fatalf("ReadDir failed: %v", err)
			}

			if len(entries) != 3 {
				tst.Errorf("Expected 3 entries, got %d", len(entries))
			}

			// RmDir on non-empty directory should fail
			if err := fs.RemoveDirectory(ctx, "/data", false); err == nil {
				tst.Error("Expected error removing non-empty directory")
			}

			// Verify directory still exists
			if _, err := fs.StatMetadata(ctx, "/data"); err != nil {
				tst.Errorf("Directory should still exist, got %v", err)
			}
		})
	}
}

// TestAllBackends_NestedPaths verifies deeply nested directory and file operations
// across all backend implementations.
func TestAllBackends_NestedPaths(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// Create nested directories manually
			if err := fs.CreateDirectory(ctx, "/a"); err != nil {
				tst.Fatalf("MkDir /a failed: %v", err)
			}
			if err := fs.CreateDirectory(ctx, "/a/b"); err != nil {
				tst.Fatalf("MkDir /a/b failed: %v", err)
			}
			if err := fs.CreateDirectory(ctx, "/a/b/c"); err != nil {
				tst.Fatalf("MkDir /a/b/c failed: %v", err)
			}

			// Create nested file
			f, err := fs.OpenFile(ctx, "/a/b/c/file.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open nested file failed: %v", err)
			}
			f.Write([]byte("nested"))
			f.Close()

			// Verify file exists
			info, err := fs.StatMetadata(ctx, "/a/b/c/file.txt")
			if err != nil {
				tst.Fatalf("Stat nested file failed: %v", err)
			}

			if info.Mode.IsDir() {
				tst.Error("Expected file, got directory")
			}

			// Verify parent dirs exist
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

// TestAllBackends_ErrorCases verifies proper error handling for invalid operations
// across all backend implementations.
func TestAllBackends_ErrorCases(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// Stat non-existent file
			if _, err := fs.StatMetadata(ctx, "/nonexistent"); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist, got %v", err)
			}

			// Open non-existent file for reading
			if _, err := fs.OpenFile(ctx, "/nonexistent", data.AccessModeRead); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist on Open for read, got %v", err)
			}

			// Create directory
			if err := fs.CreateDirectory(ctx, "/testdir"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			// Try to open directory for reading
			if _, err := fs.OpenFile(ctx, "/testdir", data.AccessModeRead); err == nil {
				tst.Error("Expected error opening directory for reading")
			}

			// Try to unlink directory (should fail)
			if err := fs.UnlinkFile(ctx, "/testdir"); err == nil {
				tst.Error("Expected error unlinking directory")
			}
		})
	}
}

// TestAllBackends_StatOperations verifies file and directory stat operations
// across all backend implementations.
func TestAllBackends_StatOperations(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// Create file with known content
			f, err := fs.OpenFile(ctx, "/stattest.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}

			content := []byte("test content for stat")
			f.Write(content)
			f.Close()

			// Stat file
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

			// Create and stat directory
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

// TestAllBackends_MultipleFiles verifies handling of multiple concurrent files
// across all backend implementations.
func TestAllBackends_MultipleFiles(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// Create multiple files
			fileCount := 10
			for i := 0; i < fileCount; i++ {
				filename := fmt.Sprintf("/file%d.txt", i)
				f, err := fs.OpenFile(ctx, filename, data.AccessModeWrite|data.AccessModeCreate)
				if err != nil {
					tst.Fatalf("Open %s failed: %v", filename, err)
				}

				content := []byte(fmt.Sprintf("content for file %d", i))
				f.Write(content)
				f.Close()
			}

			// Verify all files exist and have correct content
			for i := 0; i < fileCount; i++ {
				filename := fmt.Sprintf("/file%d.txt", i)
				f, err := fs.OpenFile(ctx, filename, data.AccessModeRead)
				if err != nil {
					tst.Fatalf("Open %s for read failed: %v", filename, err)
				}

				got, _ := io.ReadAll(f)
				f.Close()

				expected := []byte(fmt.Sprintf("content for file %d", i))
				if !bytes.Equal(got, expected) {
					tst.Errorf("File %s: expected %q, got %q", filename, expected, got)
				}
			}
		})
	}
}

// TestAllBackends_FileAppend verifies appending to existing files
// across all backend implementations.
func TestAllBackends_FileAppend(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// Create initial file
			f, err := fs.OpenFile(ctx, "/append.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}
			f.Write([]byte("first "))
			f.Close()

			// Append to file
			f, err = fs.OpenFile(ctx, "/append.txt", data.AccessModeWrite|data.AccessModeAppend)
			if err != nil {
				tst.Fatalf("Open for append failed: %v", err)
			}
			f.Write([]byte("second"))
			f.Close()

			// Read and verify
			f, err = fs.OpenFile(ctx, "/append.txt", data.AccessModeRead)
			if err != nil {
				tst.Fatalf("Open for read failed: %v", err)
			}
			defer f.Close()

			got, _ := io.ReadAll(f)
			expected := []byte("first second")

			if !bytes.Equal(got, expected) {
				tst.Errorf("Expected %q, got %q", expected, got)
			}
		})
	}
}

// TestAllBackends_FileTruncate verifies truncating existing files
// across all backend implementations.
func TestAllBackends_FileTruncate(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/", false)

			// Create initial file
			f, err := fs.OpenFile(ctx, "/trunc.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}
			f.Write([]byte("original content"))
			f.Close()

			// Truncate and write new content
			f, err = fs.OpenFile(ctx, "/trunc.txt", data.AccessModeWrite|data.AccessModeTrunc)
			if err != nil {
				tst.Fatalf("Open for truncate failed: %v", err)
			}
			f.Write([]byte("new"))
			f.Close()

			// Read and verify
			f, err = fs.OpenFile(ctx, "/trunc.txt", data.AccessModeRead)
			if err != nil {
				tst.Fatalf("Open for read failed: %v", err)
			}
			defer f.Close()

			got, _ := io.ReadAll(f)
			expected := []byte("new")

			if !bytes.Equal(got, expected) {
				tst.Errorf("Expected %q, got %q", expected, got)
			}
		})
	}
}

// TestAllBackends_EmptyDirectory verifies empty directory operations
// across all backend implementations.
func TestAllBackends_EmptyDirectory(t *testing.T) {
	factories := GetTestBackendFactories()

	for name, factory := range factories {
		t.Run(name, func(tst *testing.T) {
			ctx := tst.Context()
			fs, _ := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend initialization failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", backend); err != nil {
				tst.Fatalf("Failed to mount primary backend: %v", err)
			}

			// Create empty directory
			if err := fs.CreateDirectory(ctx, "/empty"); err != nil {
				tst.Fatalf("Failed to create empty directory: %v", err)
			}

			// List empty directory
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

			// Verify directory doesn't exist anymore
			if _, err := fs.StatMetadata(ctx, "/empty"); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist after deletion, but got '%v'", err)
			}

			if err := fs.Unmount(ctx, "/", false); err != nil {
				tst.Fatalf("failed to unmount primary backend: %v", err)
			}
		})
	}
}

// TestVFS_CloseMethod verifies that the Close method properly unmounts all filesystems
func TestVFS_CloseMethod(t *testing.T) {
	ctx := t.Context()
	fs, _ := vfs.NewVfs()

	// Mount multiple backends
	if err := fs.Mount(ctx, "/", memory.NewMemoryBackend()); err != nil {
		t.Fatalf("Failed to mount root: %v", err)
	}

	if err := fs.Mount(ctx, "/data", memory.NewMemoryBackend()); err != nil {
		t.Fatalf("Failed to mount /data: %v", err)
	}

	if err := fs.Mount(ctx, "/data/nested", memory.NewMemoryBackend()); err != nil {
		t.Fatalf("Failed to mount /data/nested: %v", err)
	}

	// Create some files to ensure backends are working
	f, err := fs.OpenFile(ctx, "/test.txt", data.AccessModeWrite|data.AccessModeCreate)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	f.Close()

	// Close should unmount all filesystems
	if err := fs.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// After close, operations should fail because nothing is mounted
	_, err = fs.StatMetadata(ctx, "/test.txt")
	if err == nil {
		t.Error("Expected error after Close, but operation succeeded")
	}
}
