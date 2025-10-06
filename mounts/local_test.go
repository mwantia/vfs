package mounts

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mwantia/vfs"
)

func TestLocalMount_FileOperations(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	// Create temp directory
	tmpDir := t.TempDir()

	if err := fs.Mount("/", NewLocal(tmpDir)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create file
	w, err := fs.Create(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
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
	r, err := fs.Open(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
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
	if err := fs.Remove(ctx, "/test.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify removed from disk
	if _, err := os.Stat(realPath); !os.IsNotExist(err) {
		t.Error("File still exists on disk after Remove")
	}
}

func TestLocalMount_DirectoryOperations(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	tmpDir := t.TempDir()

	if err := fs.Mount("/", NewLocal(tmpDir)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create directory
	if err := fs.Mkdir(ctx, "/mydir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
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
		w, err := fs.Create(ctx, "/mydir/"+name)
		if err != nil {
			t.Fatalf("Create %s failed: %v", name, err)
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

	// RemoveAll
	if err := fs.RemoveAll(ctx, "/mydir"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify removed from disk
	if _, err := os.Stat(realPath); !os.IsNotExist(err) {
		t.Error("Directory still exists after RemoveAll")
	}
}

func TestLocalMount_ExistingFiles(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	tmpDir := t.TempDir()

	// Create file on disk before mounting
	testFile := filepath.Join(tmpDir, "existing.txt")
	if err := os.WriteFile(testFile, []byte("pre-existing"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Mount and read existing file
	if err := fs.Mount("/", NewLocal(tmpDir)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	r, err := fs.Open(ctx, "/existing.txt")
	if err != nil {
		t.Fatalf("Open existing file failed: %v", err)
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
	w, err := fs.Create(ctx, "/existing.txt")
	if err != nil {
		t.Fatalf("Create for update failed: %v", err)
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
	ctx := context.Background()
	fs := vfs.NewVfs()

	tmpDir := t.TempDir()

	if err := fs.Mount("/", NewLocal(tmpDir)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Open non-existent file
	if _, err := fs.Open(ctx, "/nonexistent"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}

	// Create directory
	if err := fs.Mkdir(ctx, "/dir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Try to open directory
	if _, err := fs.Open(ctx, "/dir"); err != vfs.ErrIsDirectory {
		t.Errorf("Expected ErrIsDirectory, got %v", err)
	}

	// Try to remove directory as file
	if err := fs.Remove(ctx, "/dir"); err != vfs.ErrIsDirectory {
		t.Errorf("Expected ErrIsDirectory on Remove, got %v", err)
	}
}
