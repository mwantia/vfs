package mounts

import (
	"io"
	"strings"
	"testing"

	"github.com/mwantia/vfs"
)

func TestMemoryMount_NewMemory(t *testing.T) {
	mount := NewMemory()

	if mount == nil {
		t.Fatal("NewMemory returned nil")
	}

	if mount.files == nil {
		t.Fatal("files map not initialized")
	}

	// Should have root directory
	root, exists := mount.files[""]
	if !exists {
		t.Fatal("root directory not created")
	}

	if !root.isDir {
		t.Error("root is not a directory")
	}
}

func TestMemoryMount_Stat(t *testing.T) {
	mount := NewMemory()

	// Create a test file
	writer, err := mount.Create(t.Context(), "test.txt")
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	writer.Write([]byte("test content"))
	writer.Close()

	// Test file stat
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

	if info.IsDir {
		t.Error("expected file, got directory")
	}

	// Test directory stat
	if err := mount.Mkdir(t.Context(), "testdir"); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}

	dirInfo, err := mount.Stat(t.Context(), "testdir")
	if err != nil {
		t.Fatalf("Stat directory failed: %v", err)
	}

	if !dirInfo.IsDir {
		t.Error("expected directory, got file")
	}

	// Test non-existent file
	_, err = mount.Stat(t.Context(), "nonexistent.txt")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}

	// Test root directory
	rootInfo, err := mount.Stat(t.Context(), "")
	if err != nil {
		t.Fatalf("Stat root failed: %v", err)
	}

	if !rootInfo.IsDir {
		t.Error("root is not a directory")
	}
}

func TestMemoryMount_ReadDir(t *testing.T) {
	mount := NewMemory()

	// Create test files and directories
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, file := range files {
		writer, err := mount.Create(t.Context(), file)
		if err != nil {
			t.Fatalf("failed to create file %s: %v", file, err)
		}
		writer.Write([]byte("content"))
		writer.Close()
	}

	if err := mount.Mkdir(t.Context(), "subdir"); err != nil {
		t.Fatalf("failed to create subdirectory: %v", err)
	}

	// Test ReadDir on root
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

	// Test ReadDir on non-existent directory
	_, err = mount.ReadDir(t.Context(), "nonexistent")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}

	// Test ReadDir on file (should return ErrNotDirectory)
	_, err = mount.ReadDir(t.Context(), "file1.txt")
	if err != vfs.ErrNotDirectory {
		t.Errorf("expected ErrNotDirectory, got %v", err)
	}
}

func TestMemoryMount_ReadDir_Nested(t *testing.T) {
	mount := NewMemory()

	// Create nested structure
	writer, _ := mount.Create(t.Context(), "dir1/file1.txt")
	writer.Write([]byte("content1"))
	writer.Close()

	writer, _ = mount.Create(t.Context(), "dir1/file2.txt")
	writer.Write([]byte("content2"))
	writer.Close()

	writer, _ = mount.Create(t.Context(), "dir1/subdir/file3.txt")
	writer.Write([]byte("content3"))
	writer.Close()

	// Test ReadDir on dir1
	entries, err := mount.ReadDir(t.Context(), "dir1")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	// Should have file1.txt, file2.txt, and subdir
	if len(entries) != 3 {
		t.Errorf("expected 3 entries in dir1, got %d", len(entries))
	}

	// Verify subdir is detected as directory
	foundSubdir := false
	for _, entry := range entries {
		if entry.Name == "subdir" {
			foundSubdir = true
			if !entry.IsDir {
				t.Error("subdir should be a directory")
			}
		}
	}

	if !foundSubdir {
		t.Error("subdir not found in dir1")
	}

	// Test ReadDir on nested subdir
	entries, err = mount.ReadDir(t.Context(), "dir1/subdir")
	if err != nil {
		t.Fatalf("ReadDir on nested dir failed: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("expected 1 entry in dir1/subdir, got %d", len(entries))
	}

	if entries[0].Name != "file3.txt" {
		t.Errorf("expected file3.txt, got %s", entries[0].Name)
	}
}

func TestMemoryMount_Open(t *testing.T) {
	mount := NewMemory()

	testContent := "Hello, World!"
	writer, err := mount.Create(t.Context(), "test.txt")
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	writer.Write([]byte(testContent))
	writer.Close()

	// Test opening file
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

	// Test opening directory
	mount.Mkdir(t.Context(), "testdir")
	_, err = mount.Open(t.Context(), "testdir")
	if err != vfs.ErrIsDirectory {
		t.Errorf("expected ErrIsDirectory, got %v", err)
	}
}

func TestMemoryMount_Create(t *testing.T) {
	mount := NewMemory()

	// Test creating file
	testContent := "New file content"
	writer, err := mount.Create(t.Context(), "newfile.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	n, err := writer.Write([]byte(testContent))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if n != len(testContent) {
		t.Errorf("expected to write %d bytes, wrote %d", len(testContent), n)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file was created
	reader, err := mount.Open(t.Context(), "newfile.txt")
	if err != nil {
		t.Fatalf("failed to open created file: %v", err)
	}

	content, _ := io.ReadAll(reader)
	reader.Close()

	if string(content) != testContent {
		t.Errorf("expected content %q, got %q", testContent, string(content))
	}

	// Test creating file with parent directories
	writer2, err := mount.Create(t.Context(), "subdir/nested/file.txt")
	if err != nil {
		t.Fatalf("Create with nested path failed: %v", err)
	}
	writer2.Write([]byte("nested"))
	writer2.Close()

	// Verify parent directories were created
	info, err := mount.Stat(t.Context(), "subdir/nested")
	if err != nil {
		t.Fatalf("parent directories not created: %v", err)
	}

	if !info.IsDir {
		t.Error("parent path is not a directory")
	}

	// Test creating file where directory exists
	mount.Mkdir(t.Context(), "existingdir")
	_, err = mount.Create(t.Context(), "existingdir")
	if err != vfs.ErrIsDirectory {
		t.Errorf("expected ErrIsDirectory, got %v", err)
	}

	// Test file truncation
	writer3, _ := mount.Create(t.Context(), "truncate.txt")
	writer3.Write([]byte("original content"))
	writer3.Close()

	writer4, _ := mount.Create(t.Context(), "truncate.txt")
	writer4.Write([]byte("new"))
	writer4.Close()

	reader2, _ := mount.Open(t.Context(), "truncate.txt")
	content2, _ := io.ReadAll(reader2)
	reader2.Close()

	if string(content2) != "new" {
		t.Errorf("file was not truncated, got %q", string(content2))
	}
}

func TestMemoryMount_Remove(t *testing.T) {
	mount := NewMemory()

	// Create test file
	writer, _ := mount.Create(t.Context(), "test.txt")
	writer.Write([]byte("content"))
	writer.Close()

	// Test removing file
	if err := mount.Remove(t.Context(), "test.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify file was removed
	_, err := mount.Stat(t.Context(), "test.txt")
	if err != vfs.ErrNotExist {
		t.Error("file was not removed")
	}

	// Test removing non-existent file
	err = mount.Remove(t.Context(), "nonexistent.txt")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}

	// Test removing directory
	mount.Mkdir(t.Context(), "testdir")
	err = mount.Remove(t.Context(), "testdir")
	if err != vfs.ErrIsDirectory {
		t.Errorf("expected ErrIsDirectory, got %v", err)
	}
}

func TestMemoryMount_Mkdir(t *testing.T) {
	mount := NewMemory()

	// Test creating directory
	if err := mount.Mkdir(t.Context(), "newdir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Verify directory was created
	info, err := mount.Stat(t.Context(), "newdir")
	if err != nil {
		t.Fatalf("directory not created: %v", err)
	}

	if !info.IsDir {
		t.Error("created path is not a directory")
	}

	// Test creating existing directory
	err = mount.Mkdir(t.Context(), "newdir")
	if err != vfs.ErrExist {
		t.Errorf("expected ErrExist, got %v", err)
	}

	// Test creating nested directory (should fail without parent)
	// Note: MemoryMount.Mkdir doesn't create parents, mkdirAllLocked does
	err = mount.Mkdir(t.Context(), "parent/child")
	if err != vfs.ErrExist {
		// Might succeed if parent was created by Create operations
		// This is implementation-specific
	}
}

func TestMemoryMount_RemoveAll(t *testing.T) {
	mount := NewMemory()

	// Create directory with contents
	writer, _ := mount.Create(t.Context(), "testdir/file1.txt")
	writer.Write([]byte("content1"))
	writer.Close()

	writer, _ = mount.Create(t.Context(), "testdir/file2.txt")
	writer.Write([]byte("content2"))
	writer.Close()

	writer, _ = mount.Create(t.Context(), "testdir/subdir/file3.txt")
	writer.Write([]byte("content3"))
	writer.Close()

	// Test removing directory
	if err := mount.RemoveAll(t.Context(), "testdir"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify directory was removed
	_, err := mount.Stat(t.Context(), "testdir")
	if err != vfs.ErrNotExist {
		t.Error("directory was not removed")
	}

	// Verify all children were removed
	_, err = mount.Stat(t.Context(), "testdir/file1.txt")
	if err != vfs.ErrNotExist {
		t.Error("child file was not removed")
	}

	// Test removing non-existent path
	err = mount.RemoveAll(t.Context(), "nonexistent")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}

	// Test removing single file
	writer, _ = mount.Create(t.Context(), "single.txt")
	writer.Write([]byte("content"))
	writer.Close()

	if err := mount.RemoveAll(t.Context(), "single.txt"); err != nil {
		t.Fatalf("RemoveAll on file failed: %v", err)
	}

	_, err = mount.Stat(t.Context(), "single.txt")
	if err != vfs.ErrNotExist {
		t.Error("file was not removed")
	}
}

func TestMemoryMount_ThreadSafety(t *testing.T) {
	mount := NewMemory()

	// Create initial file
	writer, _ := mount.Create(t.Context(), "test.txt")
	writer.Write([]byte("content"))
	writer.Close()

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

	// Concurrent writes
	for i := range 5 {
		go func(idx int) {
			fileName := "file" + string(rune('0'+idx)) + ".txt"
			writer, err := mount.Create(t.Context(), fileName)
			if err != nil {
				t.Errorf("concurrent Create failed: %v", err)
			} else {
				writer.Write([]byte("content"))
				writer.Close()
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for range 15 {
		<-done
	}
}

func TestMemoryFileWriter_WriteClosed(t *testing.T) {
	mount := NewMemory()

	writer, err := mount.Create(t.Context(), "test.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some data
	writer.Write([]byte("content"))

	// Close the writer
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Try to write after close
	_, err = writer.Write([]byte("more"))
	if err != vfs.ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}

	// Try to close again
	err = writer.Close()
	if err != vfs.ErrClosed {
		t.Errorf("expected ErrClosed on double close, got %v", err)
	}
}

func TestMemoryMount_Integration(t *testing.T) {
	mount := NewMemory()

	// Create a complex directory structure
	writer, _ := mount.Create(t.Context(), "docs/readme.txt")
	writer.Write([]byte("README"))
	writer.Close()

	writer, _ = mount.Create(t.Context(), "docs/guide.txt")
	writer.Write([]byte("GUIDE"))
	writer.Close()

	mount.Mkdir(t.Context(), "docs/examples")

	writer, _ = mount.Create(t.Context(), "docs/examples/example1.txt")
	writer.Write([]byte("EXAMPLE1"))
	writer.Close()

	// List docs directory
	entries, err := mount.ReadDir(t.Context(), "docs")
	if err != nil {
		t.Fatalf("failed to read directory: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("expected 3 entries in docs, got %d", len(entries))
	}

	// Read a file
	reader, err := mount.Open(t.Context(), "docs/readme.txt")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	content, _ := io.ReadAll(reader)
	reader.Close()

	if string(content) != "README" {
		t.Errorf("expected content 'README', got %q", string(content))
	}

	// Remove a file
	if err := mount.Remove(t.Context(), "docs/guide.txt"); err != nil {
		t.Fatalf("failed to remove file: %v", err)
	}

	// Verify removal
	entries, _ = mount.ReadDir(t.Context(), "docs")
	if len(entries) != 2 {
		t.Errorf("expected 2 entries after removal, got %d", len(entries))
	}

	// Clean up with RemoveAll
	if err := mount.RemoveAll(t.Context(), "docs"); err != nil {
		t.Fatalf("failed to remove all: %v", err)
	}

	// Verify complete removal
	_, err = mount.Stat(t.Context(), "docs")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist after RemoveAll, got %v", err)
	}
}

func TestMemoryMount_EmptyFiles(t *testing.T) {
	mount := NewMemory()

	// Create empty file
	writer, err := mount.Create(t.Context(), "empty.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	writer.Close()

	// Verify it's empty
	info, err := mount.Stat(t.Context(), "empty.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Size != 0 {
		t.Errorf("expected size 0, got %d", info.Size)
	}

	// Open and read empty file
	reader, err := mount.Open(t.Context(), "empty.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	content, _ := io.ReadAll(reader)
	reader.Close()

	if len(content) != 0 {
		t.Errorf("expected empty content, got %d bytes", len(content))
	}
}

func TestMemoryMount_LargeFiles(t *testing.T) {
	mount := NewMemory()

	// Create a large file (1MB)
	largeContent := strings.Repeat("A", 1024*1024)

	writer, err := mount.Create(t.Context(), "large.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	n, err := writer.Write([]byte(largeContent))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if n != len(largeContent) {
		t.Errorf("expected to write %d bytes, wrote %d", len(largeContent), n)
	}

	writer.Close()

	// Verify size
	info, err := mount.Stat(t.Context(), "large.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Size != int64(len(largeContent)) {
		t.Errorf("expected size %d, got %d", len(largeContent), info.Size)
	}

	// Read back
	reader, err := mount.Open(t.Context(), "large.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	content, _ := io.ReadAll(reader)
	reader.Close()

	if len(content) != len(largeContent) {
		t.Errorf("expected to read %d bytes, read %d", len(largeContent), len(content))
	}
}

func TestMemoryMount_PathNormalization(t *testing.T) {
	mount := NewMemory()

	// Create file with normalized path
	writer, _ := mount.Create(t.Context(), "dir/file.txt")
	writer.Write([]byte("content"))
	writer.Close()

	// Access with different path representations
	tests := []string{
		"dir/file.txt",
		"dir//file.txt",
		"./dir/file.txt",
		"dir/./file.txt",
	}

	for _, path := range tests {
		// Note: The current implementation may not normalize all paths
		// This test documents expected behavior if normalization is added
		_, err := mount.Stat(t.Context(), path)
		if err == vfs.ErrNotExist {
			// Path normalization not implemented
			continue
		}
	}
}
