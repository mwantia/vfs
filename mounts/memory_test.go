package mounts

import (
	"bytes"
	"io"
	"testing"

	"github.com/mwantia/vfs"
)

// TestMemoryMount_FileOperations verifies basic file create, write, and read operations
// on the in-memory filesystem.
func TestMemoryMount_FileOperations(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	if err := fs.Mount(ctx, "/", NewMemory()); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create and write file
	w, err := fs.OpenWrite(ctx, "/test.txt", vfs.AccessModeWrite|vfs.AccessModeCreate)
	if err != nil {
		t.Fatalf("OpenWrite failed: %v", err)
	}

	data := []byte("hello world")
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read file
	r, err := fs.OpenRead(ctx, "/test.txt", vfs.AccessModeRead)
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

	// Verify removed
	if _, err := fs.Stat(ctx, "/test.txt"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}
}

func TestMemoryMount_DirectoryOperations(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	if err := fs.Mount(ctx, "/", NewMemory()); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create directory
	if err := fs.MkDir(ctx, "/data"); err != nil {
		t.Fatalf("MkDir failed: %v", err)
	}

	// Create files in directory
	for i, name := range []string{"file1.txt", "file2.txt", "file3.txt"} {
		w, err := fs.OpenWrite(ctx, "/data/"+name, vfs.AccessModeWrite|vfs.AccessModeCreate)
		if err != nil {
			t.Fatalf("OpenWrite %s failed: %v", name, err)
		}
		w.Write([]byte{byte(i)})
		w.Close()
	}

	// List directory
	entries, err := fs.ReadDir(ctx, "/data")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// RmDir with force (delete all)
	if err := fs.RmDir(ctx, "/data"); err == nil {
		t.Error("Expected error removing non-empty directory without force")
	}

	// Verify directory still exists
	if _, err := fs.Stat(ctx, "/data"); err != nil {
		t.Errorf("Directory should still exist, got %v", err)
	}
}

func TestMemoryMount_NestedPaths(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	if err := fs.Mount(ctx, "/", NewMemory()); err != nil {
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
	w, err := fs.OpenWrite(ctx, "/a/b/c/file.txt", vfs.AccessModeWrite|vfs.AccessModeCreate)
	if err != nil {
		t.Fatalf("OpenWrite nested file failed: %v", err)
	}
	w.Write([]byte("nested"))
	w.Close()

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

func TestMemoryMount_ErrorCases(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	if err := fs.Mount(ctx, "/", NewMemory()); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Stat non-existent file
	if _, err := fs.Stat(ctx, "/nonexistent"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}

	// OpenRead non-existent file
	if _, err := fs.OpenRead(ctx, "/nonexistent", vfs.AccessModeRead); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist on OpenRead, got %v", err)
	}

	// Create directory
	if err := fs.MkDir(ctx, "/testdir"); err != nil {
		t.Fatalf("MkDir failed: %v", err)
	}

	// Try to read directory
	if _, err := fs.OpenRead(ctx, "/testdir", vfs.AccessModeRead); err == nil {
		t.Error("Expected error opening directory for reading")
	}

	// Try to unlink directory (should fail)
	if err := fs.Unlink(ctx, "/testdir"); err == nil {
		t.Error("Expected error unlinking directory")
	}
}
