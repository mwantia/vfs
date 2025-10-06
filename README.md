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
- **Longest-prefix matching**: Automatic path resolution to the correct mount
- **Nested mounts**: Mount filesystems within other filesystems
- **Thread-safe**: Safe for concurrent access
- **Storage agnostic**: Works with any backend (S3, databases, memory, local filesystem, etc.)
- **Simple interface**: Only 7 methods to implement
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
    // Create a new VFS
    fs := vfs.NewVfs()

    // Mount a read-only memory filesystem at root
    root := mounts.NewMemory()
    fs.Mount("/", root, vfs.WithReadOnly(true))

    // Mount your custom handler
    myHandler := &MyCustomHandler{}
    fs.Mount("/data", myHandler, vfs.WithType("custom"))

    // Use the VFS
    info, err := fs.Stat(context.Background(), "/data/file.txt")
    if err != nil {
        panic(err)
    }

    fmt.Printf("File size: %d bytes\n", info.Size())
}
```

## Core Concepts

### Mounts

A **mount** is a storage backend attached to a specific path in the VFS. Each mount implements the `VirtualMount` interface and handles operations for its subtree.

```go
type VirtualMount interface {
    Stat(ctx context.Context, path string) (*VirtualFileInfo, error)
    ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error)
    Open(ctx context.Context, path string) (io.ReadCloser, error)
    Create(ctx context.Context, path string) (io.WriteCloser, error)
    Remove(ctx context.Context, path string) error
    Mkdir(ctx context.Context, path string) error
    RemoveAll(ctx context.Context, path string) error
}
```

### Path Resolution

VFS uses **longest-prefix matching** to find the correct mount:

```
Mounts:
  /           → MemoryMount
  /data       → S3Mount
  /data/cache → CacheMount

Access: /data/cache/file.txt
  → Matches "/data/cache" (longest prefix)
  → Calls CacheMount.Open(ctx, "file.txt")
```

### Mount Hierarchy

Mounts can be nested:

```go
fs.Mount("/", rootMount)
fs.Mount("/data", dataMount)
fs.Mount("/data/temp", tempMount)
```

Unmounting requires removing child mounts first (safe by default).

## Implementing a Custom Mount

Here's a simple example of a custom mount:

```go
type MyMount struct {
    // Your storage backend
}

func (m *MyMount) Stat(ctx context.Context, path string) (*vfs.VirtualFileInfo, error) {
    // Return file information
    return vfs.NewFileInfo(path, 1024, 0644, time.Now(), false), nil
}

func (m *MyMount) ReadDir(ctx context.Context, path string) ([]*vfs.VirtualFileInfo, error) {
    // Return directory contents
    return []*vfs.VirtualFileInfo{
        vfs.NewFileInfo("file1.txt", 100, 0644, time.Now(), false),
        vfs.NewFileInfo("subdir", 0, 0755, time.Now(), true),
    }, nil
}

func (m *MyMount) Open(ctx context.Context, path string) (io.ReadCloser, error) {
    // Return a reader for the file
    return os.Open(path)
}

func (m *MyMount) Create(ctx context.Context, path string) (io.WriteCloser, error) {
    // Return a writer for the file
    return os.Create(path)
}

func (m *MyMount) Remove(ctx context.Context, path string) error {
    // Remove a file
    return os.Remove(path)
}

func (m *MyMount) Mkdir(ctx context.Context, path string) error {
    // Create a directory
    return os.Mkdir(path, 0755)
}

func (m *MyMount) RemoveAll(ctx context.Context, path string) error {
    // Remove directory and contents
    return os.RemoveAll(path)
}
```

## Built-in Mounts

### Memory Mount

In-memory filesystem, useful for testing or temporary root:

```go
import "github.com/mwantia/vfs/mounts"

mem := mounts.NewMemory()
fs.Mount("/", mem)
```

### ReadOnly Wrapper

Wraps any mount to make it read-only:

```go
mount := &MyMount{}
readOnly := mounts.NewReadOnly(mount)
fs.Mount("/readonly", readOnly)
```

## API Reference

### VFS Methods

```go
// Mount management
Mount(path string, mount VirtualMount, opts ...VirtualMountOption) error
Unmount(path string) error
Mounts() []VirtualMountInfo

// File operations
Stat(ctx context.Context, path string) (*VirtualFileInfo, error)
ReadDir(ctx context.Context, path string) ([]*VirtualFileInfo, error)
Open(ctx context.Context, path string) (io.ReadCloser, error)
Create(ctx context.Context, path string) (io.WriteCloser, error)
Remove(ctx context.Context, path string) error
Mkdir(ctx context.Context, path string) error
RemoveAll(ctx context.Context, path string) error
```

### Mount Options

```go
vfs.WithReadOnly(true)        // Make mount read-only
vfs.WithType("s3")           // Set mount type for metadata
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
```

## Examples

### Example 1: S3 Backend

```go
type S3Mount struct {
    bucket string
    client *s3.Client
}

func (m *S3Mount) Stat(ctx context.Context, path string) (*vfs.VirtualFileInfo, error) {
    obj, err := m.client.HeadObject(ctx, &s3.HeadObjectInput{
        Bucket: &m.bucket,
        Key:    &path,
    })
    if err != nil {
        return nil, vfs.ErrNotExist
    }

    return vfs.NewFileInfo(path, *obj.ContentLength, 0644, *obj.LastModified, false), nil
}

// ... implement other methods
```

### Example 2: Database-backed Virtual Files

```go
type DBMount struct {
    db *sql.DB
}

func (m *DBMount) ReadDir(ctx context.Context, path string) ([]*vfs.VirtualFileInfo, error) {
    rows, err := m.db.QueryContext(ctx,
        "SELECT name, size, modified FROM files WHERE parent = ?", path)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var infos []*vfs.VirtualFileInfo
    for rows.Next() {
        var name string
        var size int64
        var modTime time.Time
        rows.Scan(&name, &size, &modTime)
        infos = append(infos, vfs.NewFileInfo(name, size, 0644, modTime, false))
    }

    return infos, nil
}

// ... implement other methods
```

### Example 3: Filter/Query-based Virtual Filesystem

```go
type FilterMount struct {
    metadata MetadataStore
    query    string
}

func (m *FilterMount) ReadDir(ctx context.Context, path string) ([]*vfs.VirtualFileInfo, error) {
    // Execute query to find matching files
    files, err := m.metadata.Query(ctx, m.query)
    if err != nil {
        return nil, err
    }

    // Convert to VirtualFileInfo
    var infos []*vfs.VirtualFileInfo
    for _, f := range files {
        infos = append(infos, vfs.NewFileInfo(f.Name, f.Size, 0644, f.ModTime, false))
    }

    return infos, nil
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
- Small API surface (7 methods)
- No external dependencies
- Easy to understand and implement

### Flexibility
- Not opinionated about storage
- Works with any backend
- Compose handlers for complex scenarios

### Safety
- Thread-safe by default
- Prevents accidental unmounting of busy mounts
- Clear error types

### Unix-like
- Familiar mount metaphor
- Longest-prefix matching
- Everything is a file

## Testing

```go
func TestMyMount(t *testing.T) {
    fs := vfs.NewVfs()
    mount := &MyMount{}

    err := fs.Mount("/test", mount)
    if err != nil {
        t.Fatal(err)
    }

    info, err := fs.Stat(context.Background(), "/test/file.txt")
    if err != nil {
        t.Fatal(err)
    }

    if info.Size() != 1024 {
        t.Errorf("expected size 1024, got %d", info.Size())
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