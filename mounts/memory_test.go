package mounts

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/mwantia/vfs"
)

func TestMemoryMount_FileOperations(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	if err := fs.Mount("/", NewMemory()); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create file
	w, err := fs.Create(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	data := []byte("hello world")
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read file
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

	// Verify removed
	if _, err := fs.Stat(ctx, "/test.txt"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}
}

func TestMemoryMount_DirectoryOperations(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	if err := fs.Mount("/", NewMemory()); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create directory
	if err := fs.Mkdir(ctx, "/data"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Create files in directory
	for i, name := range []string{"file1.txt", "file2.txt", "file3.txt"} {
		w, err := fs.Create(ctx, "/data/"+name)
		if err != nil {
			t.Fatalf("Create %s failed: %v", name, err)
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

	// RemoveAll
	if err := fs.RemoveAll(ctx, "/data"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify removed
	if _, err := fs.Stat(ctx, "/data"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist after RemoveAll, got %v", err)
	}
}

func TestMemoryMount_NestedPaths(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	if err := fs.Mount("/", NewMemory()); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create nested file (should auto-create parent dirs)
	w, err := fs.Create(ctx, "/a/b/c/file.txt")
	if err != nil {
		t.Fatalf("Create nested file failed: %v", err)
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
	ctx := context.Background()
	fs := vfs.NewVfs()

	if err := fs.Mount("/", NewMemory()); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Stat non-existent file
	if _, err := fs.Stat(ctx, "/nonexistent"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist, got %v", err)
	}

	// Open non-existent file
	if _, err := fs.Open(ctx, "/nonexistent"); err != vfs.ErrNotExist {
		t.Errorf("Expected ErrNotExist on Open, got %v", err)
	}

	// Create directory
	if err := fs.Mkdir(ctx, "/testdir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Try to open directory
	if _, err := fs.Open(ctx, "/testdir"); err != vfs.ErrIsDirectory {
		t.Errorf("Expected ErrIsDirectory, got %v", err)
	}

	// Try to remove directory as file
	if err := fs.Remove(ctx, "/testdir"); err != vfs.ErrIsDirectory {
		t.Errorf("Expected ErrIsDirectory on Remove, got %v", err)
	}
}
