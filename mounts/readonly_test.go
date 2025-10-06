package mounts

import (
	"io"
	"testing"

	"github.com/mwantia/vfs"
)

func TestReadOnlyMount_NewReadOnly(t *testing.T) {
	underlying := NewMemory()
	mount := NewReadOnly(underlying)

	if mount == nil {
		t.Fatal("NewReadOnly returned nil")
	}

	if mount.mount != underlying {
		t.Error("underlying mount not set correctly")
	}
}

func TestReadOnlyMount_Stat(t *testing.T) {
	underlying := NewMemory()

	// Create a file in underlying mount
	writer, _ := underlying.Create(t.Context(), "test.txt")
	writer.Write([]byte("test content"))
	writer.Close()

	// Wrap with read-only
	mount := NewReadOnly(underlying)

	// Test stat (should work)
	info, err := mount.Stat(t.Context(), "test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Name != "test.txt" {
		t.Errorf("expected name 'test.txt', got %s", info.Name)
	}

	if info.Size != 12 {
		t.Errorf("expected size 12, got %d", info.Size)
	}

	// Test stat on non-existent file
	_, err = mount.Stat(t.Context(), "nonexistent.txt")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestReadOnlyMount_ReadDir(t *testing.T) {
	underlying := NewMemory()

	// Create files in underlying mount
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, file := range files {
		writer, _ := underlying.Create(t.Context(), file)
		writer.Write([]byte("content"))
		writer.Close()
	}

	underlying.Mkdir(t.Context(), "subdir")

	// Wrap with read-only
	mount := NewReadOnly(underlying)

	// Test ReadDir (should work)
	entries, err := mount.ReadDir(t.Context(), "")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 4 {
		t.Errorf("expected 4 entries, got %d", len(entries))
	}

	// Verify entries
	foundFiles := make(map[string]bool)
	for _, entry := range entries {
		foundFiles[entry.Name] = true
	}

	for _, file := range files {
		if !foundFiles[file] {
			t.Errorf("missing file: %s", file)
		}
	}

	if !foundFiles["subdir"] {
		t.Error("missing subdirectory")
	}
}

func TestReadOnlyMount_Open(t *testing.T) {
	underlying := NewMemory()

	// Create file in underlying mount
	testContent := "Hello, World!"
	writer, _ := underlying.Create(t.Context(), "test.txt")
	writer.Write([]byte(testContent))
	writer.Close()

	// Wrap with read-only
	mount := NewReadOnly(underlying)

	// Test opening file (should work)
	reader, err := mount.Open(t.Context(), "test.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

	if string(content) != testContent {
		t.Errorf("expected content %q, got %q", testContent, string(content))
	}

	// Test opening non-existent file
	_, err = mount.Open(t.Context(), "nonexistent.txt")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestReadOnlyMount_Create(t *testing.T) {
	underlying := NewMemory()
	mount := NewReadOnly(underlying)

	// Test creating file (should fail)
	_, err := mount.Create(t.Context(), "newfile.txt")
	if err != vfs.ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}

	// Verify file was not created in underlying mount
	_, err = underlying.Stat(t.Context(), "newfile.txt")
	if err != vfs.ErrNotExist {
		t.Error("file should not exist in underlying mount")
	}
}

func TestReadOnlyMount_Remove(t *testing.T) {
	underlying := NewMemory()

	// Create file in underlying mount
	writer, _ := underlying.Create(t.Context(), "test.txt")
	writer.Write([]byte("content"))
	writer.Close()

	// Wrap with read-only
	mount := NewReadOnly(underlying)

	// Test removing file (should fail)
	err := mount.Remove(t.Context(), "test.txt")
	if err != vfs.ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}

	// Verify file still exists in underlying mount
	_, err = underlying.Stat(t.Context(), "test.txt")
	if err != nil {
		t.Error("file should still exist in underlying mount")
	}
}

func TestReadOnlyMount_Mkdir(t *testing.T) {
	underlying := NewMemory()
	mount := NewReadOnly(underlying)

	// Test creating directory (should fail)
	err := mount.Mkdir(t.Context(), "newdir")
	if err != vfs.ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}

	// Verify directory was not created in underlying mount
	_, err = underlying.Stat(t.Context(), "newdir")
	if err != vfs.ErrNotExist {
		t.Error("directory should not exist in underlying mount")
	}
}

func TestReadOnlyMount_RemoveAll(t *testing.T) {
	underlying := NewMemory()

	// Create directory with contents in underlying mount
	writer, _ := underlying.Create(t.Context(), "testdir/file1.txt")
	writer.Write([]byte("content1"))
	writer.Close()

	writer, _ = underlying.Create(t.Context(), "testdir/file2.txt")
	writer.Write([]byte("content2"))
	writer.Close()

	// Wrap with read-only
	mount := NewReadOnly(underlying)

	// Test removing directory (should fail)
	err := mount.RemoveAll(t.Context(), "testdir")
	if err != vfs.ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}

	// Verify directory still exists in underlying mount
	_, err = underlying.Stat(t.Context(), "testdir")
	if err != nil {
		t.Error("directory should still exist in underlying mount")
	}
}

func TestReadOnlyMount_Integration(t *testing.T) {
	underlying := NewMemory()

	// Set up complex structure in underlying mount
	writer, _ := underlying.Create(t.Context(), "docs/readme.txt")
	writer.Write([]byte("README"))
	writer.Close()

	writer, _ = underlying.Create(t.Context(), "docs/guide.txt")
	writer.Write([]byte("GUIDE"))
	writer.Close()

	underlying.Mkdir(t.Context(), "docs/examples")

	writer, _ = underlying.Create(t.Context(), "docs/examples/example1.txt")
	writer.Write([]byte("EXAMPLE1"))
	writer.Close()

	// Wrap with read-only
	mount := NewReadOnly(underlying)

	// Test read operations (should work)
	entries, err := mount.ReadDir(t.Context(), "docs")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}

	reader, err := mount.Open(t.Context(), "docs/readme.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	content, _ := io.ReadAll(reader)
	reader.Close()

	if string(content) != "README" {
		t.Errorf("expected content 'README', got %q", string(content))
	}

	// Test write operations (should all fail)
	_, err = mount.Create(t.Context(), "docs/newfile.txt")
	if err != vfs.ErrReadOnly {
		t.Errorf("Create: expected ErrReadOnly, got %v", err)
	}

	err = mount.Remove(t.Context(), "docs/readme.txt")
	if err != vfs.ErrReadOnly {
		t.Errorf("Remove: expected ErrReadOnly, got %v", err)
	}

	err = mount.Mkdir(t.Context(), "docs/newdir")
	if err != vfs.ErrReadOnly {
		t.Errorf("Mkdir: expected ErrReadOnly, got %v", err)
	}

	err = mount.RemoveAll(t.Context(), "docs")
	if err != vfs.ErrReadOnly {
		t.Errorf("RemoveAll: expected ErrReadOnly, got %v", err)
	}

	// Verify underlying mount is unchanged
	entries2, _ := underlying.ReadDir(t.Context(), "docs")
	if len(entries2) != 3 {
		t.Error("underlying mount was modified")
	}
}

func TestReadOnlyMount_WithLocalMount(t *testing.T) {
	// Test read-only wrapper with local filesystem
	tempDir := t.TempDir()
	underlying := NewLocal(tempDir)

	// Create file in underlying mount
	writer, err := underlying.Create(t.Context(), "test.txt")
	if err != nil {
		t.Fatalf("failed to create file in underlying mount: %v", err)
	}
	writer.Write([]byte("test content"))
	writer.Close()

	// Wrap with read-only
	mount := NewReadOnly(underlying)

	// Test reading (should work)
	reader, err := mount.Open(t.Context(), "test.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	content, _ := io.ReadAll(reader)
	reader.Close()

	if string(content) != "test content" {
		t.Errorf("expected content 'test content', got %q", string(content))
	}

	// Test writing (should fail)
	_, err = mount.Create(t.Context(), "newfile.txt")
	if err != vfs.ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}

	err = mount.Remove(t.Context(), "test.txt")
	if err != vfs.ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}

func TestReadOnlyMount_ErrorPropagation(t *testing.T) {
	underlying := NewMemory()
	mount := NewReadOnly(underlying)

	// Test that read errors from underlying mount propagate correctly

	// Non-existent file stat
	_, err := mount.Stat(t.Context(), "nonexistent.txt")
	if err != vfs.ErrNotExist {
		t.Errorf("Stat: expected ErrNotExist, got %v", err)
	}

	// Non-existent directory ReadDir
	_, err = mount.ReadDir(t.Context(), "nonexistent")
	if err != vfs.ErrNotExist {
		t.Errorf("ReadDir: expected ErrNotExist, got %v", err)
	}

	// Non-existent file Open
	_, err = mount.Open(t.Context(), "nonexistent.txt")
	if err != vfs.ErrNotExist {
		t.Errorf("Open: expected ErrNotExist, got %v", err)
	}

	// Create directory and try to open it
	underlying.Mkdir(t.Context(), "testdir")

	_, err = mount.Open(t.Context(), "testdir")
	if err != vfs.ErrIsDirectory {
		t.Errorf("Open directory: expected ErrIsDirectory, got %v", err)
	}

	// Try to ReadDir a file
	writer, _ := underlying.Create(t.Context(), "file.txt")
	writer.Write([]byte("content"))
	writer.Close()

	_, err = mount.ReadDir(t.Context(), "file.txt")
	if err != vfs.ErrNotDirectory {
		t.Errorf("ReadDir file: expected ErrNotDirectory, got %v", err)
	}
}

func TestReadOnlyMount_NestedReadOnly(t *testing.T) {
	// Test wrapping a read-only mount with another read-only mount
	underlying := NewMemory()

	// Create file in underlying
	writer, _ := underlying.Create(t.Context(), "test.txt")
	writer.Write([]byte("content"))
	writer.Close()

	// Double wrap
	ro1 := NewReadOnly(underlying)
	ro2 := NewReadOnly(ro1)

	// Test reading (should work)
	info, err := ro2.Stat(t.Context(), "test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Name != "test.txt" {
		t.Errorf("expected name 'test.txt', got %s", info.Name)
	}

	// Test writing (should fail)
	_, err = ro2.Create(t.Context(), "newfile.txt")
	if err != vfs.ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}

func TestReadOnlyMount_ThreadSafety(t *testing.T) {
	underlying := NewMemory()

	// Create file in underlying
	writer, _ := underlying.Create(t.Context(), "test.txt")
	writer.Write([]byte("content"))
	writer.Close()

	mount := NewReadOnly(underlying)
	done := make(chan bool)

	// Concurrent reads
	for range 10 {
		go func() {
			_, err := mount.Stat(t.Context(), "test.txt")
			if err != nil {
				t.Errorf("concurrent Stat failed: %v", err)
			}
			done <- true
		}()
	}

	// Concurrent write attempts (should all fail)
	for i := range 5 {
		go func(idx int) {
			fileName := "file" + string(rune('0'+idx)) + ".txt"
			_, err := mount.Create(t.Context(), fileName)
			if err != vfs.ErrReadOnly {
				t.Errorf("expected ErrReadOnly, got %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for range 15 {
		<-done
	}
}

func TestReadOnlyMount_UnderlyingModifications(t *testing.T) {
	// Test that modifications to underlying mount are visible through read-only wrapper
	underlying := NewMemory()

	// Create file in underlying
	writer, _ := underlying.Create(t.Context(), "test.txt")
	writer.Write([]byte("original"))
	writer.Close()

	mount := NewReadOnly(underlying)

	// Read through wrapper
	reader, _ := mount.Open(t.Context(), "test.txt")
	content, _ := io.ReadAll(reader)
	reader.Close()

	if string(content) != "original" {
		t.Errorf("expected 'original', got %q", string(content))
	}

	// Modify through underlying
	writer, _ = underlying.Create(t.Context(), "test.txt")
	writer.Write([]byte("modified"))
	writer.Close()

	// Read again through wrapper
	reader, _ = mount.Open(t.Context(), "test.txt")
	content, _ = io.ReadAll(reader)
	reader.Close()

	if string(content) != "modified" {
		t.Errorf("expected 'modified', got %q", string(content))
	}

	// Add new file through underlying
	writer, _ = underlying.Create(t.Context(), "newfile.txt")
	writer.Write([]byte("new"))
	writer.Close()

	// Check it's visible through wrapper
	info, err := mount.Stat(t.Context(), "newfile.txt")
	if err != nil {
		t.Fatalf("new file not visible: %v", err)
	}

	if info.Name != "newfile.txt" {
		t.Error("new file has wrong name")
	}
}
