package backend_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/backend"
	"github.com/mwantia/vfs/backend/memory"
	"github.com/mwantia/vfs/backend/sqlite"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
)

// TestBackendFactory creates a new backend instance for testing.
type TestBackendFactory func(t *testing.T) (backend.VirtualObjectStorageBackend, error)

// GetTestBackendFactories returns all backend implementations to test.
func GetTestBackendFactories() map[string]TestBackendFactory {
	return map[string]TestBackendFactory{
		"memory": func(t *testing.T) (backend.VirtualObjectStorageBackend, error) {
			return memory.NewMemoryBackend(""), nil
		},
		"sqlite": func(t *testing.T) (backend.VirtualObjectStorageBackend, error) {
			return sqlite.NewSQLiteBackend(":memory:")
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Create and write file
			f, err := fs.Open(ctx, "/test.txt", data.AccessModeWrite|data.AccessModeCreate)
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
			f, err = fs.Open(ctx, "/test.txt", data.AccessModeRead)
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
			if err := fs.Unlink(ctx, "/test.txt"); err != nil {
				tst.Fatalf("Unlink failed: %v", err)
			}

			// Verify removed
			if _, err := fs.Stat(ctx, "/test.txt"); err != data.ErrNotExist {
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Create directory
			if err := fs.MkDir(ctx, "/data"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			// Create files in directory
			for i, name := range []string{"file1.txt", "file2.txt", "file3.txt"} {
				f, err := fs.Open(ctx, "/data/"+name, data.AccessModeWrite|data.AccessModeCreate)
				if err != nil {
					tst.Fatalf("Open %s failed: %v", name, err)
				}
				f.Write([]byte{byte(i)})
				f.Close()
			}

			// List directory
			entries, err := fs.ReadDir(ctx, "/data")
			if err != nil {
				tst.Fatalf("ReadDir failed: %v", err)
			}

			if len(entries) != 3 {
				tst.Errorf("Expected 3 entries, got %d", len(entries))
			}

			// RmDir on non-empty directory should fail
			if err := fs.RmDir(ctx, "/data"); err == nil {
				tst.Error("Expected error removing non-empty directory")
			}

			// Verify directory still exists
			if _, err := fs.Stat(ctx, "/data"); err != nil {
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Create nested directories manually
			if err := fs.MkDir(ctx, "/a"); err != nil {
				tst.Fatalf("MkDir /a failed: %v", err)
			}
			if err := fs.MkDir(ctx, "/a/b"); err != nil {
				tst.Fatalf("MkDir /a/b failed: %v", err)
			}
			if err := fs.MkDir(ctx, "/a/b/c"); err != nil {
				tst.Fatalf("MkDir /a/b/c failed: %v", err)
			}

			// Create nested file
			f, err := fs.Open(ctx, "/a/b/c/file.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open nested file failed: %v", err)
			}
			f.Write([]byte("nested"))
			f.Close()

			// Verify file exists
			info, err := fs.Stat(ctx, "/a/b/c/file.txt")
			if err != nil {
				tst.Fatalf("Stat nested file failed: %v", err)
			}

			if info.Mode.IsDir() {
				tst.Error("Expected file, got directory")
			}

			// Verify parent dirs exist
			info, err = fs.Stat(ctx, "/a/b")
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Stat non-existent file
			if _, err := fs.Stat(ctx, "/nonexistent"); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist, got %v", err)
			}

			// Open non-existent file for reading
			if _, err := fs.Open(ctx, "/nonexistent", data.AccessModeRead); err != data.ErrNotExist {
				tst.Errorf("Expected ErrNotExist on Open for read, got %v", err)
			}

			// Create directory
			if err := fs.MkDir(ctx, "/testdir"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			// Try to open directory for reading
			if _, err := fs.Open(ctx, "/testdir", data.AccessModeRead); err == nil {
				tst.Error("Expected error opening directory for reading")
			}

			// Try to unlink directory (should fail)
			if err := fs.Unlink(ctx, "/testdir"); err == nil {
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Create file with known content
			f, err := fs.Open(ctx, "/stattest.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}

			content := []byte("test content for stat")
			f.Write(content)
			f.Close()

			// Stat file
			info, err := fs.Stat(ctx, "/stattest.txt")
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
			if err := fs.MkDir(ctx, "/statdir"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			dirInfo, err := fs.Stat(ctx, "/statdir")
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Create multiple files
			fileCount := 10
			for i := 0; i < fileCount; i++ {
				filename := fmt.Sprintf("/file%d.txt", i)
				f, err := fs.Open(ctx, filename, data.AccessModeWrite|data.AccessModeCreate)
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
				f, err := fs.Open(ctx, filename, data.AccessModeRead)
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Create initial file
			f, err := fs.Open(ctx, "/append.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}
			f.Write([]byte("first "))
			f.Close()

			// Append to file
			f, err = fs.Open(ctx, "/append.txt", data.AccessModeWrite|data.AccessModeAppend)
			if err != nil {
				tst.Fatalf("Open for append failed: %v", err)
			}
			f.Write([]byte("second"))
			f.Close()

			// Read and verify
			f, err = fs.Open(ctx, "/append.txt", data.AccessModeRead)
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Create initial file
			f, err := fs.Open(ctx, "/trunc.txt", data.AccessModeWrite|data.AccessModeCreate)
			if err != nil {
				tst.Fatalf("Open for write failed: %v", err)
			}
			f.Write([]byte("original content"))
			f.Close()

			// Truncate and write new content
			f, err = fs.Open(ctx, "/trunc.txt", data.AccessModeWrite|data.AccessModeTrunc)
			if err != nil {
				tst.Fatalf("Open for truncate failed: %v", err)
			}
			f.Write([]byte("new"))
			f.Close()

			// Read and verify
			f, err = fs.Open(ctx, "/trunc.txt", data.AccessModeRead)
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
			fs := vfs.NewVfs()

			backend, err := factory(t)
			if err != nil {
				tst.Fatalf("Backend init failed: %v", err)
			}

			mount, err := mount.NewVirtualMount("/", backend)
			if err != nil {
				tst.Fatalf("Mount creation failed: %v", err)
			}

			if err := fs.Mount(ctx, "/", mount); err != nil {
				tst.Fatalf("Mount failed: %v", err)
			}
			defer fs.Unmount(ctx, "/")

			// Create empty directory
			if err := fs.MkDir(ctx, "/emptydir"); err != nil {
				tst.Fatalf("MkDir failed: %v", err)
			}

			// List empty directory
			entries, err := fs.ReadDir(ctx, "/emptydir")
			if err != nil {
				tst.Fatalf("ReadDir failed: %v", err)
			}

			if len(entries) != 0 {
				tst.Errorf("Expected 0 entries in empty directory, got %d", len(entries))
			}

			// RmDir should fail with ErrIsDirectory
			if err := fs.RmDir(ctx, "/emptydir"); err != data.ErrIsDirectory {
				tst.Errorf("Expected ErrIsDirectory when removing directory, got %v", err)
			}

			// Verify directory still exists
			if _, err := fs.Stat(ctx, "/emptydir"); err != nil {
				tst.Errorf("Directory should still exist after failed RmDir, got %v", err)
			}
		})
	}
}
