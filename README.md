# VFS - Virtual Filesystem Library

A generic, mount-based virtual filesystem library for Go that provides a Unix-like VFS abstraction layer. Mount any storage backend as a filesystem using a simple interface.

> [!WARNING]
> This project is currently in **early development** and is **not ready for production use**. \
> Core components like the gRPC server, testing infrastructure, and production storage backends are incomplete. \
> Do not attempt to use this in any production or critical environment. \
> This `README.md`, as well as all docs have been created with AI. \
> Since the project is still in its initial state with everything open to changes.

## Overview

VFS provides a clean abstraction for building virtual filesystems with multiple mount points. Similar to Unix VFS, everything is a mount - including the root. The library handles path resolution and mount management, while you provide the storage implementation.

## Features

- **Mount-based architecture**: Everything is a mountpoint, just like Unix VFS
- **Offset-based I/O**: Efficient streaming with partial reads/writes, no full-file buffering
- **Longest-prefix matching**: Automatic path resolution to the correct mount
- **Nested mounts**: Mount filesystems within other filesystems
- **Thread-safe**: Safe for concurrent access
- **Storage agnostic**: Works with any backend (S3, databases, memory, local filesystem, etc.)
- **Simple interface**: Only 8 core methods to implement
- **No dependencies**: Pure Go, stdlib only

## Installation

```bash
go get github.com/mwantia/vfs
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/mwantia/vfs"
    "github.com/mwantia/vfs/mounts"
)

func main() {
    ctx := context.Background()

    // Create a new VFS
    fs := vfs.NewVfs()

    // Mount an in-memory filesystem at root
    root := mounts.NewMemory()
    fs.Mount(ctx, "/", root)

    // Write a file
    w, _ := fs.OpenWrite(ctx, "/data/file.txt")
    w.Write([]byte("Hello, VFS!"))
    w.Close()

    // Read the file
    r, _ := fs.OpenRead(ctx, "/data/file.txt")
    data, _ := io.ReadAll(r)
    r.Close()

    fmt.Printf("Content: %s\n", data)
}
```

## Core Concepts

### Mounts

A **mount** is a storage backend attached to a specific path in the VFS. Each mount implements the `VirtualMount` interface and handles operations for its subtree.

```go
type VirtualMount interface {
    // Metadata operations
    GetCapabilities() VirtualMountCapabilities
    Stat(ctx context.Context, path string) (*VirtualObjectInfo, error)
    List(ctx context.Context, path string) ([]*VirtualObjectInfo, error)

    // I/O operations (offset-based)
    Read(ctx context.Context, path string, offset int64, data []byte) (int, error)
    Write(ctx context.Context, path string, offset int64, data []byte) (int, error)

    // File/directory management
    Create(ctx context.Context, path string, isDir bool) error
    Delete(ctx context.Context, path string, force bool) error
    Truncate(ctx context.Context, path string, size int64) error
}
```

### Path Resolution

VFS uses **longest-prefix matching** to find the correct mount and converts absolute VFS paths to mount-relative paths:

```
Mounts:
  /           → MemoryMount
  /data       → S3Mount
  /data/cache → CacheMount

Access: /data/cache/file.txt
  → Matches "/data/cache" (longest prefix)
  → Strips mount prefix: "file.txt"
  → Calls CacheMount.Read(ctx, "file.txt", offset, data)
```

All paths passed to mount implementations are **relative to the mount point**, never absolute VFS paths.

### Mount Hierarchy

Mounts can be nested:

```go
fs.Mount("/", rootMount)
fs.Mount("/data", dataMount)
fs.Mount("/data/temp", tempMount)
```

Unmounting requires removing child mounts first (safe by default).

## Implementing a Custom Mount

Here's a simple example of a custom mount that stores files in memory:

```go
type MyMount struct {
    mu    sync.RWMutex
    files map[string][]byte
}

func (m *MyMount) GetCapabilities() vfs.VirtualMountCapabilities {
    return vfs.VirtualMountCapabilities{
        Capabilities: []vfs.VirtualMountCapability{
            vfs.VirtualMountCapabilityCRUD,
        },
    }
}

func (m *MyMount) Stat(ctx context.Context, path string) (*vfs.VirtualObjectInfo, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()

    data, exists := m.files[path]
    if !exists {
        return nil, vfs.ErrNotExist
    }

    return &vfs.VirtualObjectInfo{
        Path:    path,
        Name:    filepath.Base(path),
        Type:    vfs.ObjectTypeFile,
        Size:    int64(len(data)),
        Mode:    0644,
        ModTime: time.Now(),
    }, nil
}

func (m *MyMount) List(ctx context.Context, path string) ([]*vfs.VirtualObjectInfo, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()

    var infos []*vfs.VirtualObjectInfo
    for p, data := range m.files {
        if filepath.Dir(p) == path {
            infos = append(infos, &vfs.VirtualObjectInfo{
                Path:    p,
                Name:    filepath.Base(p),
                Type:    vfs.ObjectTypeFile,
                Size:    int64(len(data)),
                Mode:    0644,
                ModTime: time.Now(),
            })
        }
    }
    return infos, nil
}

func (m *MyMount) Read(ctx context.Context, path string, offset int64, data []byte) (int, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()

    file, exists := m.files[path]
    if !exists {
        return 0, vfs.ErrNotExist
    }

    if offset >= int64(len(file)) {
        return 0, io.EOF
    }

    n := copy(data, file[offset:])
    return n, nil
}

func (m *MyMount) Write(ctx context.Context, path string, offset int64, data []byte) (int, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    file, exists := m.files[path]
    if !exists {
        return 0, vfs.ErrNotExist
    }

    // Extend file if needed
    newSize := offset + int64(len(data))
    if newSize > int64(len(file)) {
        newFile := make([]byte, newSize)
        copy(newFile, file)
        file = newFile
    }

    copy(file[offset:], data)
    m.files[path] = file
    return len(data), nil
}

func (m *MyMount) Create(ctx context.Context, path string, isDir bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    if _, exists := m.files[path]; exists {
        return vfs.ErrExist
    }

    if !isDir {
        m.files[path] = []byte{}
    }
    return nil
}

func (m *MyMount) Delete(ctx context.Context, path string, force bool) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    if _, exists := m.files[path]; !exists {
        return vfs.ErrNotExist
    }

    delete(m.files, path)
    return nil
}

func (m *MyMount) Truncate(ctx context.Context, path string, size int64) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    file, exists := m.files[path]
    if !exists {
        return vfs.ErrNotExist
    }

    if size < int64(len(file)) {
        m.files[path] = file[:size]
    } else if size > int64(len(file)) {
        newFile := make([]byte, size)
        copy(newFile, file)
        m.files[path] = newFile
    }
    return nil
}
```

## Built-in Mounts

### Memory Mount

In-memory filesystem with full CRUD support, useful for testing:

```go
import "github.com/mwantia/vfs/mounts"

mem := mounts.NewMemory()
fs.Mount(ctx, "/", mem)

// Create and write file
w, _ := fs.OpenWrite(ctx, "/test.txt")
w.Write([]byte("data"))
w.Close()

// Read file
r, _ := fs.OpenRead(ctx, "/test.txt")
data, _ := io.ReadAll(r)
r.Close()
```

### Local Mount

Local filesystem access with offset-based I/O:

```go
import "github.com/mwantia/vfs/mounts"

local := mounts.NewLocal("/tmp/vfs-root")
fs.Mount(ctx, "/local", local)
```

### ReadOnly Wrapper

Wraps any mount to make it read-only:

```go
mount := mounts.NewMemory()
readOnly := mounts.NewReadOnly(mount)
fs.Mount(ctx, "/readonly", readOnly)

// Writes will fail with ErrReadOnly
```

## API Reference

### VFS Methods

```go
// Mount management
Mount(ctx context.Context, path string, mount VirtualMount, opts ...VirtualMountOption) error
Unmount(ctx context.Context, path string) error

// Streaming I/O (high-level)
OpenRead(ctx context.Context, path string) (io.ReadCloser, error)
OpenWrite(ctx context.Context, path string) (io.ReadWriteCloser, error)

// Direct I/O (low-level)
Read(ctx context.Context, path string, offset, size int64) ([]byte, error)
Write(ctx context.Context, path string, offset int64, data []byte) (int64, error)

// File/directory operations
Stat(ctx context.Context, path string) (*VirtualFileInfo, error)
ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error)
MkDir(ctx context.Context, path string) error
RmDir(ctx context.Context, path string) error
Unlink(ctx context.Context, path string) error
Rename(ctx context.Context, oldPath, newPath string) error
Chmod(ctx context.Context, path string, mode VirtualFileMode) error

// Advanced operations
Lookup(ctx context.Context, path string) (bool, error)
GetAttr(ctx context.Context, path string) (*VirtualObjectInfo, error)
SetAttr(ctx context.Context, path string, info VirtualObjectInfo) (bool, error)
```

### Mount Options

```go
vfs.WithReadOnly(true)        // Make mount read-only
```

### Standard Errors

```go
vfs.ErrNotMounted      // Path not mounted
vfs.ErrAlreadyMounted  // Path already has a mount
vfs.ErrMountBusy       // Mount has children, can't unmount
vfs.ErrNotExist        // File does not exist
vfs.ErrExist           // File already exists
vfs.ErrIsDirectory     // Expected file, got directory
vfs.ErrNotDirectory    // Expected directory, got file
vfs.ErrPermission      // Permission denied
vfs.ErrReadOnly        // Read-only filesystem
vfs.ErrClosed          // File already closed
```

## Examples

### Example 1: S3 Backend with Streaming

```go
type S3Mount struct {
    bucket string
    client *s3.Client
}

func (m *S3Mount) Read(ctx context.Context, path string, offset int64, data []byte) (int, error) {
    // Use S3 range reads for efficient streaming
    rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+int64(len(data))-1)

    result, err := m.client.GetObject(ctx, &s3.GetObjectInput{
        Bucket: &m.bucket,
        Key:    &path,
        Range:  &rangeHeader,
    })
    if err != nil {
        return 0, vfs.ErrNotExist
    }
    defer result.Body.Close()

    return io.ReadFull(result.Body, data)
}

func (m *S3Mount) Write(ctx context.Context, path string, offset int64, data []byte) (int, error) {
    // For simplicity, writes require uploading entire file
    // Production implementation would use multipart uploads
    _, err := m.client.PutObject(ctx, &s3.PutObjectInput{
        Bucket: &m.bucket,
        Key:    &path,
        Body:   bytes.NewReader(data),
    })
    if err != nil {
        return 0, err
    }
    return len(data), nil
}

// ... implement other methods
```

### Example 2: Database-backed Virtual Files

```go
type DBMount struct {
    db *sql.DB
}

func (m *DBMount) Read(ctx context.Context, path string, offset int64, data []byte) (int, error) {
    var content []byte
    err := m.db.QueryRowContext(ctx,
        "SELECT content FROM files WHERE path = ?", path).Scan(&content)
    if err == sql.ErrNoRows {
        return 0, vfs.ErrNotExist
    }
    if err != nil {
        return 0, err
    }

    if offset >= int64(len(content)) {
        return 0, io.EOF
    }

    n := copy(data, content[offset:])
    return n, nil
}

func (m *DBMount) Write(ctx context.Context, path string, offset int64, data []byte) (int, error) {
    // Read existing content
    var content []byte
    m.db.QueryRowContext(ctx, "SELECT content FROM files WHERE path = ?", path).Scan(&content)

    // Extend if needed
    newSize := offset + int64(len(data))
    if newSize > int64(len(content)) {
        newContent := make([]byte, newSize)
        copy(newContent, content)
        content = newContent
    }

    // Write at offset
    copy(content[offset:], data)

    // Update database
    _, err := m.db.ExecContext(ctx,
        "UPDATE files SET content = ?, size = ? WHERE path = ?",
        content, len(content), path)

    return len(data), err
}

// ... implement other methods
```

### Example 3: Streaming from HTTP Source

```go
type HTTPMount struct {
    baseURL string
    client  *http.Client
}

func (m *HTTPMount) Read(ctx context.Context, path string, offset int64, data []byte) (int, error) {
    url := m.baseURL + path
    req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
    if err != nil {
        return 0, err
    }

    // Use HTTP range requests for efficient streaming
    rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+int64(len(data))-1)
    req.Header.Set("Range", rangeHeader)

    resp, err := m.client.Do(req)
    if err != nil {
        return 0, err
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusNotFound {
        return 0, vfs.ErrNotExist
    }

    return io.ReadFull(resp.Body, data)
}

// ... implement other methods
```

## Use Cases

- **Multi-cloud storage**: Mount S3, Azure Blob, GCS under unified namespace
- **Database as filesystem**: Expose database records as virtual files
- **Filter/query views**: Dynamic "directories" based on queries
- **Testing**: Mock filesystems for unit tests
- **Caching layers**: Overlay cache over slow storage
- **Archive browsing**: Mount tar/zip as directories without extraction
- **Configuration namespaces**: Organize config from multiple sources

## Design Philosophy

### Simplicity
- Small API surface (8 core methods)
- No external dependencies
- Easy to understand and implement

### Efficiency
- Offset-based I/O for streaming large files
- No full-file buffering required
- Direct support for partial reads/writes
- Minimal memory allocations

### Flexibility
- Not opinionated about storage
- Works with any backend
- Compose handlers for complex scenarios
- Support for sparse files

### Safety
- Thread-safe by default
- Prevents accidental unmounting of busy mounts
- Clear error types
- Context-based cancellation

### Unix-like
- Familiar mount metaphor
- Longest-prefix matching
- POSIX-like I/O semantics
- Everything is a file

## Testing

```go
func TestMyMount(t *testing.T) {
    ctx := t.Context()
    fs := vfs.NewVfs()
    mount := mounts.NewMemory()

    if err := fs.Mount(ctx, "/test", mount); err != nil {
        t.Fatal(err)
    }

    // Write file
    w, err := fs.OpenWrite(ctx, "/test/file.txt")
    if err != nil {
        t.Fatal(err)
    }
    w.Write([]byte("test data"))
    w.Close()

    // Read file
    r, err := fs.OpenRead(ctx, "/test/file.txt")
    if err != nil {
        t.Fatal(err)
    }
    data, _ := io.ReadAll(r)
    r.Close()

    if string(data) != "test data" {
        t.Errorf("expected 'test data', got %q", data)
    }
}
```

## Performance Considerations

- Mount lookup is O(n) where n is number of mounts (typically small)
- Path resolution uses string prefix matching (fast)
- Read lock for resolution, write lock only for mount/unmount
- No allocations in hot path (mount resolution)

## Contributing

Contributions welcome! Please:

1. Keep the API minimal and focused
2. Maintain zero external dependencies
3. Add tests for new features
4. Follow existing code style
5. Update documentation

## License

Apache License 2.0

## Related Projects

- [afero](https://github.com/spf13/afero) - Filesystem abstraction (different approach, more POSIX-focused)
- [billy](https://github.com/go-git/go-billy) - Filesystem interface used by go-git
- [vfs (blang)](https://github.com/blang/vfs) - Virtual filesystem interface

**What makes this different:**
- Mount-based (not just interface wrapper)
- Explicit path resolution with longest-prefix matching
- Designed for virtual/layered filesystems, not just abstracting os.Filesystem
- Minimal API focused on core operations