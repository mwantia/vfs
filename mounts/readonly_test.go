package mounts

import (
	"io"
	"testing"

	"github.com/mwantia/vfs"
)

// TestReadOnlyMount_ReadOperations verifies that read operations work correctly
// on a read-only wrapped mount.
func TestReadOnlyMount_ReadOperations(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	// Create memory mount with some data
	mem := NewMemory()
	if err := fs.Mount(ctx, "/", mem); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Populate with data
	w, _ := fs.OpenWrite(ctx, "/file.txt")
	w.Write([]byte("readonly test"))
	w.Close()

	fs.MkDir(ctx, "/dir")
	w, _ = fs.OpenWrite(ctx, "/dir/nested.txt")
	w.Write([]byte("nested"))
	w.Close()

	// Unmount and remount as readonly
	fs.Unmount(ctx, "/")
	if err := fs.Mount(ctx, "/", NewReadOnly(mem)); err != nil {
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

	// OpenRead should work
	r, err := fs.OpenRead(ctx, "/file.txt")
	if err != nil {
		t.Fatalf("OpenRead failed: %v", err)
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
	ctx := t.Context()
	fs := vfs.NewVfs()

	mem := NewMemory()
	if err := fs.Mount(ctx, "/", NewReadOnly(mem)); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// OpenWrite should fail due to Create failing on readonly mount
	_, err := fs.OpenWrite(ctx, "/test.txt")
	if err != vfs.ErrReadOnly {
		t.Errorf("Expected ErrReadOnly on OpenWrite, got %v", err)
	}

	// MkDir should fail
	if err := fs.MkDir(ctx, "/dir"); err != vfs.ErrReadOnly {
		t.Errorf("Expected ErrReadOnly on MkDir, got %v", err)
	}
}

func TestReadOnlyMount_DeleteOperationsFail(t *testing.T) {
	ctx := t.Context()
	fs := vfs.NewVfs()

	// Create memory mount with data
	mem := NewMemory()
	if err := fs.Mount(ctx, "/", mem); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	w, _ := fs.OpenWrite(ctx, "/file.txt")
	w.Write([]byte("test"))
	w.Close()

	fs.MkDir(ctx, "/dir")

	// Remount as readonly
	fs.Unmount(ctx, "/")
	if err := fs.Mount(ctx, "/", NewReadOnly(mem)); err != nil {
		t.Fatalf("Mount readonly failed: %v", err)
	}

	// Unlink should fail
	if err := fs.Unlink(ctx, "/file.txt"); err != vfs.ErrReadOnly {
		t.Errorf("Expected ErrReadOnly on Unlink, got %v", err)
	}

	// RmDir should fail
	if err := fs.RmDir(ctx, "/dir"); err != vfs.ErrReadOnly {
		t.Errorf("Expected ErrReadOnly on RmDir, got %v", err)
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
