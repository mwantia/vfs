package mounts

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mwantia/vfs"
)

// TestSQLiteMount_FileOperations verifies basic file create, write, and read operations
// on the SQLite-backed filesystem.
func TestSQLiteMount_FileOperations(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	// Create SQLite mount with in-memory database
	mount, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}
	defer mount.Close()

	if err := fs.Mount(ctx, "/", mount); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create and write file
	f, err := fs.Open(ctx, "/test.txt", vfs.AccessModeWrite|vfs.AccessModeCreate)
	if err != nil {
		t.Fatalf("Open for write failed: %v", err)
	}

	data := []byte("hello world")
	if _, err := f.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read file
	f, err = fs.Open(ctx, "/test.txt", vfs.AccessModeRead)
	if err != nil {
		t.Fatalf("Open for read failed: %v", err)
	}
	defer f.Close()

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("Expected %q, got %q", data, got)
	}

	// Remove file
	if err := fs.Unlink(ctx, "/test.txt"); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	// Verify removed
	if _, err := fs.Stat(ctx, "/test.txt"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}
}

func TestSQLiteMount_DirectoryOperations(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	mount, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}
	defer mount.Close()

	if err := fs.Mount(ctx, "/", mount); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create directory
	if err := fs.MkDir(ctx, "/data"); err != nil {
		t.Fatalf("MkDir failed: %v", err)
	}

	// Create files in directory
	for i, name := range []string{"file1.txt", "file2.txt", "file3.txt"} {
		f, err := fs.Open(ctx, "/data/"+name, vfs.AccessModeWrite|vfs.AccessModeCreate)
		if err != nil {
			t.Fatalf("Open %s failed: %v", name, err)
		}
		f.Write([]byte{byte(i)})
		f.Close()
	}

	// List directory
	entries, err := fs.ReadDir(ctx, "/data")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// RmDir without force (should fail for non-empty directory)
	if err := fs.RmDir(ctx, "/data"); err == nil {
		t.Error("Expected error removing non-empty directory without force")
	}

	// Verify directory still exists
	if _, err := fs.Stat(ctx, "/data"); err != nil {
		t.Errorf("Directory should still exist, got %v", err)
	}
}

func TestSQLiteMount_NestedPaths(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	mount, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}
	defer mount.Close()

	if err := fs.Mount(ctx, "/", mount); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create nested directories manually (no auto-create in new API)
	if err := fs.MkDir(ctx, "/a"); err != nil {
		t.Fatalf("MkDir /a failed: %v", err)
	}
	if err := fs.MkDir(ctx, "/a/b"); err != nil {
		t.Fatalf("MkDir /a/b failed: %v", err)
	}
	if err := fs.MkDir(ctx, "/a/b/c"); err != nil {
		t.Fatalf("MkDir /a/b/c failed: %v", err)
	}

	// Create nested file
	f, err := fs.Open(ctx, "/a/b/c/file.txt", vfs.AccessModeWrite|vfs.AccessModeCreate)
	if err != nil {
		t.Fatalf("Open nested file failed: %v", err)
	}
	f.Write([]byte("nested"))
	f.Close()

	// Verify file exists
	info, err := fs.Stat(ctx, "/a/b/c/file.txt")
	if err != nil {
		t.Fatalf("Stat nested file failed: %v", err)
	}

	if info.IsDir {
		t.Error("Expected file, got directory")
	}

	// Verify parent dirs exist
	info, err = fs.Stat(ctx, "/a/b")
	if err != nil {
		t.Fatalf("Stat parent dir failed: %v", err)
	}

	if !info.IsDir {
		t.Error("Expected directory, got file")
	}
}

func TestSQLiteMount_ErrorCases(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	mount, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}
	defer mount.Close()

	if err := fs.Mount(ctx, "/", mount); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Stat non-existent file
	if _, err := fs.Stat(ctx, "/nonexistent"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}

	// Open non-existent file for reading
	if _, err := fs.Open(ctx, "/nonexistent", vfs.AccessModeRead); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist on Open for read, got %v", err)
	}

	// Create directory
	if err := fs.MkDir(ctx, "/testdir"); err != nil {
		t.Fatalf("MkDir failed: %v", err)
	}

	// Try to open directory for reading
	if _, err := fs.Open(ctx, "/testdir", vfs.AccessModeRead); err == nil {
		t.Error("Expected error opening directory for reading")
	}

	// Try to unlink directory (should fail)
	if err := fs.Unlink(ctx, "/testdir"); err == nil {
		t.Error("Expected error unlinking directory")
	}
}

func TestSQLiteMount_Persistence(t *testing.T) {
	ctx := t.Context()

	// Create temporary database file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// First mount - create and write data
	fs1 := vfs.NewVfs()
	mount1, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}

	if err := fs1.Mount(ctx, "/", mount1); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create test data
	f, err := fs1.Open(ctx, "/persistent.txt", vfs.AccessModeWrite|vfs.AccessModeCreate)
	if err != nil {
		t.Fatalf("Open for write failed: %v", err)
	}

	testData := []byte("persistent data")
	f.Write(testData)
	f.Close()

	// Create directory with file
	fs1.MkDir(ctx, "/dir")
	f, _ = fs1.Open(ctx, "/dir/nested.txt", vfs.AccessModeWrite|vfs.AccessModeCreate)
	f.Write([]byte("nested persistent"))
	f.Close()

	// Unmount and close
	fs1.Unmount(ctx, "/")

	// Second mount - verify data persisted
	fs2 := vfs.NewVfs()
	mount2, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatalf("NewSQLite (second) failed: %v", err)
	}
	defer mount2.Close()

	if err := fs2.Mount(ctx, "/", mount2); err != nil {
		t.Fatalf("Mount (second) failed: %v", err)
	}

	// Verify file exists
	info, err := fs2.Stat(ctx, "/persistent.txt")
	if err != nil {
		t.Fatalf("Stat persistent file failed: %v", err)
	}

	if info.Size != int64(len(testData)) {
		t.Errorf("Expected size %d, got %d", len(testData), info.Size)
	}

	// Read and verify content
	f, err = fs2.Open(ctx, "/persistent.txt", vfs.AccessModeRead)
	if err != nil {
		t.Fatalf("Open for read failed: %v", err)
	}

	got, _ := io.ReadAll(f)
	f.Close()

	if !bytes.Equal(got, testData) {
		t.Errorf("Expected %q, got %q", testData, got)
	}

	// Verify nested file
	f, err = fs2.Open(ctx, "/dir/nested.txt", vfs.AccessModeRead)
	if err != nil {
		t.Fatalf("Open nested file failed: %v", err)
	}

	got, _ = io.ReadAll(f)
	f.Close()

	if !bytes.Equal(got, []byte("nested persistent")) {
		t.Errorf("Expected 'nested persistent', got %q", got)
	}
}

func TestSQLiteMount_LargeFile(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	mount, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}
	defer mount.Close()

	if err := fs.Mount(ctx, "/", mount); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create large file (1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	f, err := fs.Open(ctx, "/large.bin", vfs.AccessModeWrite|vfs.AccessModeCreate)
	if err != nil {
		t.Fatalf("Open for write failed: %v", err)
	}

	n, err := f.Write(largeData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if n != len(largeData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(largeData), n)
	}

	f.Close()

	// Verify size
	info, err := fs.Stat(ctx, "/large.bin")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Size != int64(len(largeData)) {
		t.Errorf("Expected size %d, got %d", len(largeData), info.Size)
	}

	// Read back and verify
	f, err = fs.Open(ctx, "/large.bin", vfs.AccessModeRead)
	if err != nil {
		t.Fatalf("Open for read failed: %v", err)
	}
	defer f.Close()

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, largeData) {
		t.Error("Large file data mismatch")
	}
}

func TestSQLiteMount_MountLifecycle(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	mount, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}

	// Mount should succeed
	if err := fs.Mount(ctx, "/data", mount); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create some data
	f, _ := fs.Open(ctx, "/data/test.txt", vfs.AccessModeWrite|vfs.AccessModeCreate)
	f.Write([]byte("test"))
	f.Close()

	// Unmount should succeed
	if err := fs.Unmount(ctx, "/data"); err != nil {
		t.Fatalf("Unmount failed: %v", err)
	}

	// After unmount, database should be closed
	// Trying to use the mount directly should fail
	_, err = mount.Stat(ctx, "test.txt")
	if err == nil {
		t.Error("Expected error after unmount, got nil")
	}
}

func TestSQLiteMount_FilePersistence(t *testing.T) {
	ctx := t.Context()

	// Use a real file path
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "vfs_test.db")

	// Ensure cleanup
	defer os.Remove(dbPath)

	mount, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}
	defer mount.Close()

	fs := vfs.NewVfs()
	if err := fs.Mount(ctx, "/", mount); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Verify the database file was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}
}

func TestSQLiteMount_Capabilities(t *testing.T) {
	mount, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("NewSQLite failed: %v", err)
	}
	defer mount.Close()

	caps := mount.GetCapabilities()

	expectedCaps := []vfs.VirtualMountCapability{
		vfs.VirtualMountCapabilityCRUD,
		vfs.VirtualMountCapabilityMetadata,
		vfs.VirtualMountCapabilityQuery,
	}

	if len(caps.Capabilities) != len(expectedCaps) {
		t.Errorf("Expected %d capabilities, got %d", len(expectedCaps), len(caps.Capabilities))
	}

	// Verify each expected capability exists
	for _, expected := range expectedCaps {
		found := false
		for _, cap := range caps.Capabilities {
			if cap == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected capability %v not found", expected)
		}
	}
}
