package mounts

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mwantia/vfs"
)

// TestLocalMount_FileOperations verifies that file operations work correctly
// with the local filesystem mount, including actual disk persistence.
func TestLocalMount_FileOperations(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	// Create temp directory
	tmpDir := t.TempDir()

	if err := fs.Mount(ctx, "/", NewLocal(tmpDir)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create and write file
	f, err := fs.Open(ctx, "/test.txt", vfs.AccessModeWrite|vfs.AccessModeCreate)
	if err != nil {
		t.Fatalf("Open for write failed: %v", err)
	}

	data := []byte("local filesystem")
	if _, err := f.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file exists on actual filesystem
	realPath := filepath.Join(tmpDir, "test.txt")
	if _, err := os.Stat(realPath); err != nil {
		t.Fatalf("File not created on disk: %v", err)
	}

	// Read file via VFS
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

	// Verify removed from disk
	if _, err := os.Stat(realPath); !os.IsNotExist(err) {
		t.Error("File still exists on disk after Unlink")
	}
}

func TestLocalMount_DirectoryOperations(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	tmpDir := t.TempDir()

	if err := fs.Mount(ctx, "/", NewLocal(tmpDir)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create directory
	if err := fs.MkDir(ctx, "/mydir"); err != nil {
		t.Fatalf("MkDir failed: %v", err)
	}

	// Verify exists on disk
	realPath := filepath.Join(tmpDir, "mydir")
	info, err := os.Stat(realPath)
	if err != nil {
		t.Fatalf("Directory not created on disk: %v", err)
	}
	if !info.IsDir() {
		t.Error("Created path is not a directory")
	}

	// Create files in directory
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		f, err := fs.Open(ctx, "/mydir/"+name, vfs.AccessModeWrite|vfs.AccessModeCreate)
		if err != nil {
			t.Fatalf("Open %s failed: %v", name, err)
		}
		f.Close()
	}

	// List directory
	entries, err := fs.ReadDir(ctx, "/mydir")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// RmDir on non-empty directory should fail
	if err := fs.RmDir(ctx, "/mydir"); err == nil {
		t.Error("Expected error removing non-empty directory")
	}

	// Verify directory still exists on disk
	if _, err := os.Stat(realPath); err != nil {
		t.Error("Directory should still exist after failed RmDir")
	}
}

func TestLocalMount_ExistingFiles(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	tmpDir := t.TempDir()

	// Create file on disk before mounting
	testFile := filepath.Join(tmpDir, "existing.txt")
	if err := os.WriteFile(testFile, []byte("pre-existing"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Mount and read existing file
	if err := fs.Mount(ctx, "/", NewLocal(tmpDir)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	f, err := fs.Open(ctx, "/existing.txt", vfs.AccessModeRead)
	if err != nil {
		t.Fatalf("Open existing file failed: %v", err)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	f.Close() // Close before opening for write

	if string(data) != "pre-existing" {
		t.Errorf("Expected 'pre-existing', got %q", data)
	}

	// Update file via VFS
	f, err = fs.Open(ctx, "/existing.txt", vfs.AccessModeWrite|vfs.AccessModeTrunc)
	if err != nil {
		t.Fatalf("Open for update failed: %v", err)
	}
	f.Write([]byte("updated"))
	f.Close()

	// Verify update on disk
	updated, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read updated file: %v", err)
	}

	if string(updated) != "updated" {
		t.Errorf("Expected 'updated', got %q", updated)
	}
}

func TestLocalMount_ErrorCases(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	tmpDir := t.TempDir()

	if err := fs.Mount(ctx, "/", NewLocal(tmpDir)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Open non-existent file for reading
	if _, err := fs.Open(ctx, "/nonexistent", vfs.AccessModeRead); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}

	// Create directory
	if err := fs.MkDir(ctx, "/dir"); err != nil {
		t.Fatalf("MkDir failed: %v", err)
	}

	// Try to open directory for reading
	if _, err := fs.Open(ctx, "/dir", vfs.AccessModeRead); err == nil {
		t.Error("Expected error opening directory for reading")
	}

	// Try to unlink directory (should fail)
	if err := fs.Unlink(ctx, "/dir"); err == nil {
		t.Error("Expected error unlinking directory")
	}
}
