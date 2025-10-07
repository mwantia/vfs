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
	w, err := fs.OpenWrite(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("OpenWrite failed: %v", err)
	}

	data := []byte("local filesystem")
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file exists on actual filesystem
	realPath := filepath.Join(tmpDir, "test.txt")
	if _, err := os.Stat(realPath); err != nil {
		t.Fatalf("File not created on disk: %v", err)
	}

	// Read file via VFS
	r, err := fs.OpenRead(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("OpenRead failed: %v", err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
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
		w, err := fs.OpenWrite(ctx, "/mydir/"+name)
		if err != nil {
			t.Fatalf("OpenWrite %s failed: %v", name, err)
		}
		w.Close()
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

	r, err := fs.OpenRead(ctx, "/existing.txt")
	if err != nil {
		t.Fatalf("OpenRead existing file failed: %v", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(data) != "pre-existing" {
		t.Errorf("Expected 'pre-existing', got %q", data)
	}

	// Update file via VFS
	w, err := fs.OpenWrite(ctx, "/existing.txt")
	if err != nil {
		t.Fatalf("OpenWrite for update failed: %v", err)
	}
	w.Write([]byte("updated"))
	w.Close()

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

	// OpenRead non-existent file
	if _, err := fs.OpenRead(ctx, "/nonexistent"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}

	// Create directory
	if err := fs.MkDir(ctx, "/dir"); err != nil {
		t.Fatalf("MkDir failed: %v", err)
	}

	// Try to read directory
	if _, err := fs.OpenRead(ctx, "/dir"); err == nil {
		t.Error("Expected error opening directory for reading")
	}

	// Try to unlink directory (should fail)
	if err := fs.Unlink(ctx, "/dir"); err == nil {
		t.Error("Expected error unlinking directory")
	}
}
