package mounts

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mwantia/vfs"
)

func TestLocalMount_NewLocal(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	if mount == nil {
		t.Fatal("NewLocal returned nil")
	}

	if mount.root != filepath.Clean(tempDir) {
		t.Errorf("expected root %s, got %s", filepath.Clean(tempDir), mount.root)
	}
}

func TestLocalMount_Stat(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Create a test file
	testFile := filepath.Join(tempDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

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
	testDir := filepath.Join(tempDir, "testdir")
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
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
}

func TestLocalMount_ReadDir(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Create test files and directories
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, file := range files {
		path := filepath.Join(tempDir, file)
		if err := os.WriteFile(path, []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	if err := os.Mkdir(filepath.Join(tempDir, "subdir"), 0755); err != nil {
		t.Fatalf("failed to create subdirectory: %v", err)
	}

	// Test ReadDir
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

func TestLocalMount_Open(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	testContent := "Hello, World!"
	testFile := filepath.Join(tempDir, "test.txt")
	if err := os.WriteFile(testFile, []byte(testContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

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
	if err := os.Mkdir(filepath.Join(tempDir, "testdir"), 0755); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}

	_, err = mount.Open(t.Context(), "testdir")
	if err != vfs.ErrIsDirectory {
		t.Errorf("expected ErrIsDirectory, got %v", err)
	}
}

func TestLocalMount_Create(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

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
	content, err := os.ReadFile(filepath.Join(tempDir, "newfile.txt"))
	if err != nil {
		t.Fatalf("failed to read created file: %v", err)
	}

	if string(content) != testContent {
		t.Errorf("expected content %q, got %q", testContent, string(content))
	}

	// Test creating file with parent directories
	writer2, err := mount.Create(t.Context(), "subdir/nested/file.txt")
	if err != nil {
		t.Fatalf("Create with nested path failed: %v", err)
	}
	writer2.Close()

	// Verify parent directories were created
	info, err := os.Stat(filepath.Join(tempDir, "subdir/nested"))
	if err != nil {
		t.Fatalf("parent directories not created: %v", err)
	}

	if !info.IsDir() {
		t.Error("parent path is not a directory")
	}

	// Test creating file where directory exists
	if err := os.Mkdir(filepath.Join(tempDir, "existingdir"), 0755); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}

	_, err = mount.Create(t.Context(), "existingdir")
	if err != vfs.ErrIsDirectory {
		t.Errorf("expected ErrIsDirectory, got %v", err)
	}
}

func TestLocalMount_Remove(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Create test file
	testFile := filepath.Join(tempDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Test removing file
	if err := mount.Remove(t.Context(), "test.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify file was removed
	if _, err := os.Stat(testFile); !os.IsNotExist(err) {
		t.Error("file was not removed")
	}

	// Test removing non-existent file
	err := mount.Remove(t.Context(), "nonexistent.txt")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}

	// Test removing directory
	if err := os.Mkdir(filepath.Join(tempDir, "testdir"), 0755); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}

	err = mount.Remove(t.Context(), "testdir")
	if err != vfs.ErrIsDirectory {
		t.Errorf("expected ErrIsDirectory, got %v", err)
	}
}

func TestLocalMount_Mkdir(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Test creating directory
	if err := mount.Mkdir(t.Context(), "newdir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Verify directory was created
	info, err := os.Stat(filepath.Join(tempDir, "newdir"))
	if err != nil {
		t.Fatalf("directory not created: %v", err)
	}

	if !info.IsDir() {
		t.Error("created path is not a directory")
	}

	// Test creating existing directory
	err = mount.Mkdir(t.Context(), "newdir")
	if err != vfs.ErrExist {
		t.Errorf("expected ErrExist, got %v", err)
	}

	// Test creating directory with non-existent parent
	err = mount.Mkdir(t.Context(), "parent/child")
	if err == nil {
		t.Error("expected error for non-existent parent, got nil")
	}
}

func TestLocalMount_RemoveAll(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Create directory with contents
	dirPath := filepath.Join(tempDir, "testdir")
	if err := os.Mkdir(dirPath, 0755); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}

	// Create files in directory
	for i := 1; i <= 3; i++ {
		filePath := filepath.Join(dirPath, "file"+string(rune('0'+i))+".txt")
		if err := os.WriteFile(filePath, []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	// Create subdirectory
	subDir := filepath.Join(dirPath, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("failed to create subdirectory: %v", err)
	}

	// Test removing directory
	if err := mount.RemoveAll(t.Context(), "testdir"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify directory was removed
	if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
		t.Error("directory was not removed")
	}

	// Test removing non-existent path
	err := mount.RemoveAll(t.Context(), "nonexistent")
	if err != vfs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestLocalMount_ResolvePath(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"root path", "", tempDir},
		{"simple path", "file.txt", filepath.Join(tempDir, "file.txt")},
		{"nested path", "dir/subdir/file.txt", filepath.Join(tempDir, "dir/subdir/file.txt")},
		{"path with dots", "./file.txt", filepath.Join(tempDir, "file.txt")},
		{"path with double dots", "dir/../file.txt", filepath.Join(tempDir, "file.txt")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mount.resolvePath(tt.input)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestLocalMount_ThreadSafety(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Create initial file
	testFile := filepath.Join(tempDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Run concurrent operations
	done := make(chan bool)

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func() {
			_, err := mount.Stat(t.Context(), "test.txt")
			if err != nil {
				t.Errorf("concurrent Stat failed: %v", err)
			}
			done <- true
		}()
	}

	// Concurrent writes
	for i := 0; i < 5; i++ {
		idx := i
		go func() {
			fileName := "file" + string(rune('0'+idx)) + ".txt"
			writer, err := mount.Create(t.Context(), fileName)
			if err != nil {
				t.Errorf("concurrent Create failed: %v", err)
			} else {
				writer.Write([]byte("content"))
				writer.Close()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 15; i++ {
		<-done
	}
}

func TestLocalMount_ErrorMapping(t *testing.T) {
	// Test permission errors (if possible)
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Create a read-only directory (on Unix systems)
	roDir := filepath.Join(tempDir, "readonly")
	if err := os.Mkdir(roDir, 0555); err != nil {
		t.Fatalf("failed to create read-only directory: %v", err)
	}
	defer os.Chmod(roDir, 0755) // Cleanup

	// Try to create file in read-only directory
	_, err := mount.Create(t.Context(), "readonly/file.txt")
	if err == nil {
		t.Error("expected permission error, got nil")
	}
	// Note: Exact error may vary by OS, but should contain permission information
}

func TestLocalMount_FileInfoConversion(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Create test file
	testFile := filepath.Join(tempDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	osInfo, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	vfsInfo := mount.fileInfoToVirtual(osInfo, "test.txt")

	if vfsInfo.Name != "test.txt" {
		t.Errorf("expected name 'test.txt', got %s", vfsInfo.Name)
	}

	if vfsInfo.Path != "test.txt" {
		t.Errorf("expected path 'test.txt', got %s", vfsInfo.Path)
	}

	if vfsInfo.Size != 7 {
		t.Errorf("expected size 7, got %d", vfsInfo.Size)
	}

	if vfsInfo.IsDir {
		t.Error("expected file, got directory")
	}

	// Check mode has permission bits
	if vfsInfo.Mode.Perm() == 0 {
		t.Error("mode has no permission bits")
	}
}

func TestLocalMount_Integration(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Create a complex directory structure
	writer, err := mount.Create(t.Context(), "docs/readme.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	writer.Write([]byte("README"))
	writer.Close()

	writer, err = mount.Create(t.Context(), "docs/guide.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	writer.Write([]byte("GUIDE"))
	writer.Close()

	if err := mount.Mkdir(t.Context(), "docs/examples"); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}

	writer, err = mount.Create(t.Context(), "docs/examples/example1.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
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

func TestLocalMount_PathTraversal(t *testing.T) {
	tempDir := t.TempDir()
	mount := NewLocal(tempDir)

	// Note: filepath.Join does NOT prevent path traversal with ".."
	// This is expected behavior - it's up to the application layer
	// to decide whether to allow or restrict such access.
	// The LocalMount provides direct filesystem access within its root.

	// Test that path resolution works with relative paths
	resolved := mount.resolvePath("subdir/file.txt")
	expected := filepath.Join(tempDir, "subdir/file.txt")
	if resolved != expected {
		t.Errorf("expected %s, got %s", expected, resolved)
	}

	// Test that path traversal is allowed (by design)
	// This allows accessing parent directories if the OS permits
	resolved2 := mount.resolvePath("../outside.txt")
	if !filepath.IsAbs(resolved2) {
		t.Error("resolved path should be absolute")
	}
}
