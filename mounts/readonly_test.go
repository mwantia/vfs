package mounts

import (
	"context"
	"io"
	"testing"

	"github.com/mwantia/vfs"
)

func TestReadOnlyMount_ReadOperations(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	// Create memory mount with some data
	mem := NewMemory()
	if err := fs.Mount("/", mem); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Populate with data
	w, _ := fs.Create(ctx, "/file.txt")
	w.Write([]byte("readonly test"))
	w.Close()

	fs.Mkdir(ctx, "/dir")
	w, _ = fs.Create(ctx, "/dir/nested.txt")
	w.Write([]byte("nested"))
	w.Close()

	// Unmount and remount as readonly
	fs.Unmount("/")
	if err := fs.Mount("/", NewReadOnly(mem)); err != nil {
		t.Fatalf("Mount readonly failed: %v", err)
	}

	// Stat should work
	info, err := fs.Stat(ctx, "/file.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.IsDir {
		t.Error("Expected file, got directory")
	}

	// Open should work
	r, err := fs.Open(ctx, "/file.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer r.Close()

	data, _ := io.ReadAll(r)
	if string(data) != "readonly test" {
		t.Errorf("Expected 'readonly test', got %q", data)
	}

	// Stat directory should work
	dirInfo, err := fs.Stat(ctx, "/dir")
	if err != nil {
		t.Fatalf("Stat dir failed: %v", err)
	}
	if !dirInfo.IsDir {
		t.Error("Expected directory")
	}
}

func TestReadOnlyMount_WriteOperationsFail(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	mem := NewMemory()
	if err := fs.Mount("/", NewReadOnly(mem)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Create returns a writer, but it should fail on Close
	w, err := fs.Create(ctx, "/test.txt")
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	// Write and close should fail
	w.Write([]byte("test"))
	if err := w.Close(); err != vfs.ErrReadOnly {
		t.Errorf("Expected ErrReadOnly on Close, got %v", err)
	}

	// Mkdir should fail
	if err := fs.Mkdir(ctx, "/dir"); err != vfs.ErrReadOnly {
		t.Errorf("Expected ErrReadOnly on Mkdir, got %v", err)
	}
}

func TestReadOnlyMount_DeleteOperationsFail(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewVfs()

	// Create memory mount with data
	mem := NewMemory()
	if err := fs.Mount("/", mem); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	w, _ := fs.Create(ctx, "/file.txt")
	w.Write([]byte("test"))
	w.Close()

	fs.Mkdir(ctx, "/dir")

	// Remount as readonly
	fs.Unmount("/")
	if err := fs.Mount("/", NewReadOnly(mem)); err != nil {
		t.Fatalf("Mount readonly failed: %v", err)
	}

	// Remove should fail
	if err := fs.Remove(ctx, "/file.txt"); err != vfs.ErrReadOnly {
		t.Errorf("Expected ErrReadOnly on Remove, got %v", err)
	}

	// RemoveAll should fail
	if err := fs.RemoveAll(ctx, "/dir"); err != vfs.ErrReadOnly {
		t.Errorf("Expected ErrReadOnly on RemoveAll, got %v", err)
	}

	// Verify files still exist
	if _, err := fs.Stat(ctx, "/file.txt"); err != nil {
		t.Error("File was removed despite readonly mount")
	}
	if _, err := fs.Stat(ctx, "/dir"); err != nil {
		t.Error("Directory was removed despite readonly mount")
	}
}

func TestReadOnlyMount_CapabilitiesPassthrough(t *testing.T) {
	mem := NewMemory()
	ro := NewReadOnly(mem)

	memCaps := mem.GetCapabilities()
	roCaps := ro.GetCapabilities()

	if len(memCaps.Capabilities) != len(roCaps.Capabilities) {
		t.Error("ReadOnly mount should pass through capabilities")
	}
}
