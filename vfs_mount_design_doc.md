# VFS Hierarchical Mount System - Design & Implementation Guide

## Table of Contents

1. [Overview](#overview)
2. [Problem Statement](#problem-statement)
3. [Design Principles](#design-principles)
4. [Architecture](#architecture)
5. [Core Concepts](#core-concepts)
6. [Implementation Guide](#implementation-guide)
7. [Capabilities System](#capabilities-system)
8. [Extension System](#extension-system)
9. [Error Handling](#error-handling)
10. [Future Considerations](#future-considerations)
11. [Migration Path](#migration-path)

---

## Overview

This document describes a hierarchical mount system for a virtual filesystem (VFS) that enables:

- **Decentralized mount storage:** Each backend manages its own submounts
- **Composable operations:** Mounts can be stacked with extensions (ACL, caching, etc.)
- **High abstraction:** Backends don't know about the global hierarchy
- **Scalable architecture:** From simple ephemeral in-memory mounts to complex persistent + external backends

The approach treats the VFS as a **routing/forwarding layer** and individual backends as **isolated mini-filesystems** that can mount other backends as submounts.

---

## Problem Statement

### Previous Approach

The old system stored all mount information externally:
- Mount table was separate from backend logic
- VFS had to manage all mount traversal
- Adding persistence meant retrofitting storage into a monolithic VFS
- Backends had no knowledge of being mounted or having submounts

### Issues

1. **Non-portable backends:** Backends didn't carry mount information
2. **Centralized complexity:** VFS became the bottleneck for all mount logic
3. **Persistence friction:** No natural place to store mount metadata
4. **Low abstraction:** VFS knew too much about backend internals

### New Approach

- Backends themselves manage their submounts
- Mount information is **decentralized and backend-specific**
  - Ephemeral backends store mounts in memory (map)
  - SQLite backends store mounts in a table, reconstructed on `Open(ctx)`
- VFS becomes a thin routing layer
- Extensions become transparent wrappers around backends

---

## Design Principles

### 1. **Backends Are Mini-Filesystems**

Each backend can:
- Have submounts at any path it chooses
- Serve files from its own data store
- Delegate requests to submounts when appropriate
- Fail independently without affecting other mounts

### 2. **Relative Paths Only**

Mounts never know their absolute position in the hierarchy. They only receive paths relative to their mount point.

```
Absolute: /home/user/storage/file.txt
At /home mount:  /user/storage/file.txt
At /home/user/storage mount: /file.txt
```

This keeps mounts portable and decoupled.

### 3. **Context-Driven Traversal**

A `Context` object carries:
- The **absolute path** (never changes during traversal)
- The **current relative path** (updated at each mount level)
- The **traversal level** (depth in hierarchy)
- References to the **registrar** (for caching)

New contexts are created at each `Traverse()` call, maintaining immutability and traceability.

> Important! This traversal happens towards the mountpoints, meaning that the initial request (coming to VFS) happens with the original `context.Context` and is transformed into our custom `TraversalContext` when communicating with the underlying mounts.\
> Additionally, this new context still needs to have all capabilities from the original `context.Context`.

### 4. **Capabilities Over Assumptions**

Backends declare what they can do. VFS and extensions query capabilities to decide behavior, not vice versa.

### 5. **Defer Complexity**

Symlinks, atomic multi-mount operations, concurrent modification, hot-swappingâ€”all deferred for now. Architecture supports adding them later without breaking the core.

### 6. **Extensions Are Transparent Wrappers**

ACL policies, Redis caching, rate limitingâ€”these attach as wrappers around backends, not as separate concerns in the VFS.

---

## Architecture

### Mount Hierarchy Model (Example)

```
                          ROOT (/)
                     [EphemeralBackend]
                    /        |        \
                   /         |         \
                /home     /cache      /archive
          [SQLiteBackend] [Redis]   [S3Backend]
            /      \
           /        \
        /user    /staging
    [FSBackend] [FSBackend]
```

Key properties:
- **Root is always present** and serves as entry point
- **Longest-prefix matching** for overlapping mounts
- **Exact component matching** for path lookups (e.g., `/ho` doesn't match `/home`)
- **Cascade failure:** Unmounting a parent unmounts all children

### Request Flow

```
User: VFS.OpenFile("/home/user/storage/file.txt")
  â†“
VFS:
  1. Create context with absolute path
  2. Check cache (registrar)
  3. Traverse through root mount
  â†“
Root Mount (Ephemeral):
  1. Receive relative path: "/home/user/storage/file.txt"
  2. Check submounts: found /home
  3. Delegate to SQLite mount with new context
  â†“
SQLite Mount (/home):
  1. Receive relative path: "/user/storage/file.txt"
  2. Check submounts: found /user/storage
  3. Delegate to FS mount with new context
  â†“
FS Mount (/home/user/storage):
  1. Receive relative path: "/file.txt"
  2. No submounts, serve locally
  3. Open from disk
  4. Cache result: absolute path â†’ FS mount
  â†“
Return: file handle
```

### Cache Invalidation

When unmounting `/home`:

```
Registrar cache before:
  /home/user/file.txt              â†’ SQLiteBackend
  /home/user/storage/file.txt      â†’ FSBackend
  /sys/net/devices                 â†’ ProcBackend

Unmount("/home", sqliteMount):
  1. Create context: WithAbsolute("/home")
  2. Check IsBusy(): false
  3. Call sqliteMount.Unmount(ctx)
  4. registrar.InvalidatePrefix("/home")

Registrar cache after:
  /sys/net/devices                 â†’ ProcBackend
  (all entries starting with /home removed)
```

---

## Core Concepts

### 1. Context Interface

The `Context` object is passed to every mount operation. It provides:

```go
type Context interface {
    // Path information
    GetAbsolutePath() string      // "/home/user/storage/file.txt" (never changes)
    GetRelativePath() string      // "/file.txt" at the FS mount level
    GetTraversalLevel() int        // Current depth in hierarchy
    
    // Mount operations
    Traverse(mount Backend) (Backend, string, Context, error)
        // Strips current mount level from path
        // Returns the next mount to delegate to and new context
    
    // Registration services
    GetRegistrar() Registrar      // Access to cache and mount registry
    
    // Delegation helper
    Delegate(operation Op) Result // Auto-traversal helper (optional)
}
```

**Important:** Each `Traverse()` call creates a **new context** with updated state.

### 2. Backend Interface

All backends implement the mount-aware interface:

```go
type Backend interface {
    // File operations
    OpenFile(ctx Context, path string) (File, error)
    CreateFile(ctx Context, path string) (File, error)
    DeleteFile(ctx Context, path string) error
    Stat(ctx Context, path string) (FileInfo, error)
    ReadDir(ctx Context, path string) ([]FileInfo, error)
    
    // Submount management
    RegisterSubmount(path string, mount Backend) error
    UnregisterSubmount(path string) error
    ListSubmounts() []string
    
    // Lifecycle
    Mount(ctx Context) error       // Initialize, load persistent state
    Unmount(ctx Context) error     // Cleanup, persist state
    IsBusy() bool                   // Any active requests?
    Health() HealthStatus          // Current health state
    
    // Self-description
    Capabilities() []Capability   // What this backend can do
}
```

### 3. Registrar Interface

Manages caching and mount registration:

```go
type Registrar interface {
    // Caching
    Get(absolutePath string) (Backend, bool)
    Cache(absolutePath string, backend Backend, mount Backend)
    InvalidatePrefix(pathPrefix string) error
    
    // Mount registry (private to backends)
    RegisterMount(path string, backend Backend) error
    LookupMount(path string) (Backend, bool)
    ListMounts() []MountInfo
}
```

### 4. Capability System

Backends declare what they support:

```go
type Capability struct {
    Name   string
    Params map[string]interface{}
}
```

Standard capabilities:
- `Mount`: Can have submounts
- `Persistent`: Data survives restart
- `InMemory`: Ephemeral, no persistence
- `ReadOnly`: Doesn't support write operations
- `Queryable`: Supports advanced queries (e.g., SQL)
- `Quota`: Supports storage limits
- `Versioning`: Supports file versioning
- `ACL`: Supports access control lists

**Why:** VFS and extensions query these to make decisions without hardcoding backend types.

---

## Implementation Guide

### Phase 1: Core Architecture

#### 1.1 Implement Context

```
File: context/context.go

type context struct {
    absolutePath    string
    relativePath    string
    traversalLevel  int
    registrar       Registrar
    mounts          []MountRecord  // breadcrumb trail
}

Methods:
- GetAbsolutePath() string
- GetRelativePath() string
- GetTraversalLevel() int
- Traverse(mount Backend) (Backend, string, Context, error)
  * Validates mount is registered
  * Strips next path component
  * Returns new context
- GetRegistrar() Registrar
```

**Key logic in Traverse():**
```
1. Split relative path by "/" â†’ components
2. Take first component (e.g., "user" from "/user/storage/file.txt")
3. Check if "user" has a submount registered
4. If yes: return (submount, "/storage/file.txt", newContext)
5. If no: return (current mount, original path, currentContext)
```

#### 1.2 Implement Registrar

```
File: registrar/registrar.go

type Registrar struct {
    cacheLock    sync.RWMutex
    cache        map[string]Backend    // absolute path â†’ backend
    mounts       map[string]Backend    // path prefix â†’ backend
    cacheVersion int64
}

Methods:
- Get(path) (Backend, bool): O(1) lookup
- Cache(path, backend, mount): O(1) store
- InvalidatePrefix(prefix): O(n) prefix scan and remove
- RegisterMount(path, backend) error
- LookupMount(path) (Backend, bool): longest-prefix match
- ListMounts() []MountInfo
```

#### 1.3 Update VFS Layer

```
File: vfs/vfs.go

type VirtualFileSystem struct {
    root      Backend
    registrar Registrar
}

Methods:
- OpenFile(path string) (File, error)
  1. ctx := context.WithAbsolute(path)
  2. if cached := registrar.Get(path); cached != nil { return ... }
  3. result, err := root.OpenFile(ctx, path)
  4. Cache result if successful
  5. Return

- Unmount(path string, backend Backend) error
  1. ctx := context.WithAbsolute(path)
  2. if backend.IsBusy() { return ErrBusy }
  3. backend.Unmount(ctx)
  4. registrar.InvalidatePrefix(path)
  5. Update root's submount list
```

### Phase 2: Backend Updates

#### 2.1 Ephemeral Backend

```
File: backends/ephemeral/ephemeral.go

type EphemeralBackend struct {
    files   map[string]*InMemoryFile
    mounts  map[string]Backend
    lock    sync.RWMutex
}

Methods:
- OpenFile(ctx Context, path string) (File, error)
  1. Split path into [first, rest...]
  2. if rest is empty:
     - Look up in files map
     - Return file or ErrNotFound
  3. if rest is not empty:
     - Check if first has a submount: mounts[first]
     - If yes: ctx.Traverse(submount)
     - If no: ErrNotFound (ephemeral doesn't have /home in root)

- RegisterSubmount(path string, mount Backend) error
  - mounts[path] = mount
  - return nil

- Capabilities() []Capability
  - return []{"Mount", "InMemory"}
```

#### 2.2 SQLite Backend

```
File: backends/sqlite/sqlite.go

type SQLiteBackend struct {
    db      *sql.DB
    files   string // table name for files
    mounts  string // table name for mounts
    lock    sync.RWMutex
}

Methods:
- Mount(ctx Context) error
  1. Load all rows from mounts table
  2. For each row: backend := createBackend(row)
  3. Register backend: b.mounts[path] = backend
  4. Call backend.Mount(ctx) to initialize

- Unmount(ctx Context) error
  1. For each submount: submount.Unmount(ctx)
  2. Persist current state to DB
  3. Close connections

- OpenFile(ctx Context, path string) (File, error)
  1. Split path
  2. Check submounts: if mounts[first] exists, Traverse
  3. Otherwise, query files table for [path]
  4. Return result or ErrNotFound

- Capabilities() []Capability
  - return []{"Mount", "Persistent", "Queryable"}
```

### Phase 3: Cache Integration

#### 3.1 Cache Lifecycle

```
Successful operation:
  1. Backend returns result
  2. Backend calls: ctx.GetRegistrar().Cache(ctx.GetAbsolutePath(), backend, parent)
  3. Registrar stores: cache[absolutePath] = backend

Failed operation:
  - Don't cache
  - Or cache with TTL for negative caches (optional)

Unmount:
  - registrar.InvalidatePrefix(path)
  - All entries starting with path removed from cache
```

#### 3.2 Cache Invalidation Policy

```
On file modification (Create, Delete, etc.):
  Option A: Invalidate only that file
    - registrar.Invalidate(absolutePath)
  
  Option B: Invalidate entire mount
    - registrar.InvalidatePrefix(mountPath)
  
  Recommendation: Option B (simpler, more conservative)
  
Reason: The backend knows when its own state changed.
Call invalidation in the backend's Create/Delete methods.
```

---

## Capabilities System

### Capability Declaration

Each backend declares capabilities at initialization:

```go
// Ephemeral backend
func (e *EphemeralBackend) Capabilities() []Capability {
    return []Capability{
        {Name: "Mount"},
        {Name: "InMemory"},
        {Name: "FastRead"},
    }
}

// SQLite backend
func (s *SQLiteBackend) Capabilities() []Capability {
    return []Capability{
        {Name: "Mount"},
        {Name: "Persistent"},
        {Name: "Queryable", Params: map[string]interface{}{"language": "SQL"}},
        {Name: "Quota", Params: map[string]interface{}{"maxSize": 1000000}},
    }
}
```

### VFS Using Capabilities

```go
// Decide if backend can be restarted safely
canRestart := hasCapability(backend, "Persistent")

// Decide if quotas apply
canQuota := hasCapability(backend, "Quota")
if canQuota {
    quota := backend.Capabilities()[idx].Params["maxSize"].(int64)
}

// Decide default permissions
isReadOnly := hasCapability(backend, "ReadOnly")
```

### Extending Capabilities

When attaching an extension (ACL wrapper), the wrapper declares **additional** capabilities or **masks** existing ones:

```go
// ACL wrapper around SQLiteBackend
func (a *ACLExtension) Capabilities() []Capability {
    // Inherits from wrapped backend
    base := a.wrapped.Capabilities()
    
    // Adds ACL capability
    base = append(base, Capability{Name: "ACL"})
    
    // Might mask WriteOnReadOnly if policy forbids it
    if a.policy.ForbidWrites {
        // Remove "Persistent" or add "ReadOnly"
    }
    
    return base
}
```

---

## Extension System

### Extension as Wrapper

Extensions are implemented as decorators that wrap backends:

```go
type ExtensionWrapper interface {
    Backend
    SetWrapped(backend Backend)
    GetWrapped() Backend
}
```

### Example: ACL Extension

```
File: extensions/acl/acl.go

type ACLExtension struct {
    wrapped Backend
    policy  *Policy          // Vault-style path-based policies
    token   Token            // From context during request
}

Methods:
- OpenFile(ctx Context, path string) (File, error)
  1. Check policy: ctx.GetToken() against path
  2. If denied: return ErrPermissionDenied
  3. If allowed: return wrapped.OpenFile(ctx, path)

- CreateFile(ctx Context, path string) (File, error)
  1. Check policy for write permission
  2. If allowed: return wrapped.CreateFile(ctx, path)
  3. Otherwise: return ErrPermissionDenied

- Mount(ctx Context) error
  - return wrapped.Mount(ctx)

- Unmount(ctx Context) error
  - return wrapped.Unmount(ctx)

- Capabilities() []Capability
  - base := wrapped.Capabilities()
  - return append(base, {Name: "ACL"})
```

### Example: Redis Cache Extension

```
File: extensions/cache/redis_cache.go

type RedisCacheExtension struct {
    wrapped Backend
    redis   *redis.Client
    ttl     time.Duration
}

Methods:
- OpenFile(ctx Context, path string) (File, error)
  1. cacheKey := hashPath(path)
  2. if cached := redis.Get(cacheKey); cached != nil
     - return deserialize(cached)
  3. result, err := wrapped.OpenFile(ctx, path)
  4. if err == nil:
     - redis.Set(cacheKey, serialize(result), ttl)
  5. return result, err

- Stat(ctx Context, path string) (FileInfo, error)
  - Similar: check cache, delegate, cache result

- CreateFile(...), DeleteFile(...):
  - Delegate and invalidate cache:
    - redis.Del(pathPrefix + "*")
    - OR ctx.GetRegistrar().InvalidatePrefix(path)
```

### Attaching Extensions

```go
// Attach ACL to SQLite backend
sqliteBackend := sqlite.New(...)
sqliteBackend = &acl.ACLExtension{
    wrapped: sqliteBackend,
    policy: policyLoader.Load(),
}

// Attach Redis cache
sqliteBackend = &cache.RedisCacheExtension{
    wrapped: sqliteBackend,
    redis: redisClient,
    ttl: 5 * time.Minute,
}

// Mount
vfs.Mount("/home", sqliteBackend)
```

Now requests go: VFS â†’ Redis cache â†’ ACL â†’ SQLite backend

---

## Error Handling

### Error Propagation

Errors bubble up immediately; they are not masked or retried at the VFS level:

```
Operation fails at FS backend â†’ VFS receives error immediately
```

### Common Error Scenarios

#### 1. Mount Not Found

```go
// User requests /var/log/app.log but /var/log isn't mounted
Root.OpenFile(ctx, "/var/log/app.log")
  â†’ Checks submounts: no /var mount
  â†’ Returns error: ErrNotFound or ErrNoSuchMount
```

#### 2. Backend Health Degradation

```go
// S3 backend disconnects during operation
s3Backend.OpenFile(ctx, path)
  â†’ S3 returns: ErrS3Unavailable
  â†’ Bubbles to VFS
  â†’ Caller receives error
  â†’ (Optional) VFS can query s3Backend.Health() to decide retry
```

#### 3. Cycle Detection

```go
// Admin accidentally creates cycle: /home â†’ /archive â†’ /home
Request to /home/file.txt
  â†’ Traversal hits depth limit (e.g., > 10 levels)
  â†’ Returns: ErrTraversalTooDeep
  â†’ Prevents infinite recursion
```

### Busy Unmount

```go
// Attempt unmount while backend is serving requests
VFS.Unmount("/home", sqliteBackend)
  â†’ Call sqliteBackend.IsBusy()
  â†’ Returns true (2 active requests)
  â†’ Returns error: ErrBackendBusy
  â†’ Admin must wait or force-unmount

Force-unmount (dangerous):
VFS.UnmountForce("/home", sqliteBackend)
  â†’ Calls Unmount without checking IsBusy
  â†’ Active requests may crash or hang
  â†’ Document as "use with caution"
```

---

## Future Considerations

### 1. Symlinks and Cross-Mount Traversal

**Current:** Not supported.

**Future approach:**
- Symlinks can point across mount boundaries
- When following a symlink, re-enter full path resolution
- Same context framework handles it automatically

### 2. Atomic Multi-Mount Operations

**Current:** Single mount/unmount only.

**Future approach:**
- "Mount transaction": mount multiple backends atomically
- Rollback if any mount fails
- Uses context versioning

### 3. Concurrent Modification

**Current:** Not thread-safe (deferred).

**Future approach:**
- Add generation numbers to contexts
- Mount table versioning (increment on change)
- Cache checks generation; if stale, re-resolve

### 4. Hot-Swapping Core Backends

**Current:** Not supported (by design).

**Approach:** Mark as "not planned."
- Reason: Too much complexity for marginal benefit
- Alternative: Extensions can hot-swap (ACL/Cache)
- If needed: unmount and remount (introduces downtime)

### 5. Quotas and Rate Limiting

**Current:** Declared as capability, not enforced.

**Future approach:**
- Backend declares quota in capabilities
- Wrapper extension enforces limits
- Elegant separation of concerns

### 6. Event System (Watch/Notify)

**Current:** Not implemented.

**Future approach:**
- Backends can emit "file changed" events
- VFS dispatches to watchers
- Extensions can react (e.g., invalidate cache)

---

## Migration Path

### From Old System â†’ New System

#### Step 1: Implement New Core

1. Create Context interface and implementation
2. Create Registrar (with caching)
3. Update Backend interface to accept Context
4. Keep old VFS alongside new one (dual-mode)

#### Step 2: Adapt Current Backends

1. Add `RegisterSubmount`, `UnregisterSubmount`, `ListSubmounts` to each backend
2. Add `Mount`, `Unmount` (initialize/persist logic)
3. Add `Health`, `IsBusy`
4. Add `Capabilities` declaration

#### Step 3: Migrate One Backend at a Time

1. Migrate EphemeralBackend (simple in-memory mounts)
2. Test thoroughly
3. Migrate SQLiteBackend (add mounts table)
4. Test thoroughly
5. Migrate FSBackend
6. Deprecate old system

#### Step 4: Attach Extensions

1. Implement ACL wrapper
2. Implement Cache wrapper
3. Verify they work with mounted backends
4. Update mount procedures to accept extensions

### Rollout Strategy

```
Phase 1: Code alongside old system (1-2 weeks)
  - New code path doesn't affect production
  - Internal testing only

Phase 2: Parallel testing (1-2 weeks)
  - New system serves test traffic
  - Old system still handles production
  - Compare results

Phase 3: Gradual migration (2-4 weeks)
  - New system handles small % of production traffic
  - Monitor errors, performance
  - Gradually increase percentage

Phase 4: Full transition (1 week)
  - All traffic through new system
  - Old system remains as fallback
  - Remove old system after stability window
```

---

## Best Practices

### For Backend Implementers

1. **Always normalize paths** in `OpenFile`, `CreateFile`, etc.
   - Remove `/../`, `//`, trailing slashes
   - Or trust VFS did it once at entry

2. **Check submounts before local lookup**
   - `// Check if path enters a submount`
   - `if submount := b.mounts[firstComponent]; submount != nil { ... }`
   - `// Otherwise, serve locally`

3. **Use context to cache results**
   - `ctx.GetRegistrar().Cache(ctx.GetAbsolutePath(), b, parentMount)`

4. **Implement IsBusy correctly**
   - Atomic counter or channel-based tracking
   - Return true only if requests are **actively running**, not queued

5. **Fail fast on invalid operations**
   - ReadOnly backend rejects CreateFile immediately
   - Don't queue or delay

### For Extension Implementers

1. **Implement as thin wrappers**
   - Check your concern (ACL, cache, rate limit)
   - Delegate to wrapped backend
   - Modify response if needed

2. **Preserve context fidelity**
   - Pass context unchanged to wrapped backend
   - Don't modify absolute path
   - Don't skip context creation

3. **Handle errors gracefully**
   - If your extension fails, should you retry or pass error through?
   - Document your behavior

4. **Declare capabilities accurately**
   - If your extension denies writes, declare `ReadOnly`
   - Don't hide capabilities from the wrapped backend

### For VFS Users

1. **Check capabilities before assuming features**
   - `if !hasCapability(backend, "Persistent") { warn("backend may lose data") }`

2. **Use contexts correctly**
   - Create new context per operation
   - Don't reuse stale contexts

3. **Unmount gracefully**
   - Check `IsBusy()` before unmounting
   - Plan for `ErrBackendBusy` responses

4. **Monitor health**
   - Periodically call `backend.Health()`
   - React to degraded backends (log, alert, etc.)

---

## Appendix: Quick Reference

### Context Creation

```go
ctx := context.WithAbsolute(absolutePath)
```

### Mount Registration

```go
root.RegisterSubmount("/home", sqliteBackend)
```

### Successful Operation

```go
result, err := backend.OpenFile(ctx, relativePath)
if err == nil {
    ctx.GetRegistrar().Cache(ctx.GetAbsolutePath(), backend, parent)
}
```

### Unmounting

```go
if backend.IsBusy() {
    return ErrBackendBusy
}
ctx := context.WithAbsolute("/home")
backend.Unmount(ctx)
registrar.InvalidatePrefix("/home")
```

### Adding Extension

```go
backend = &acl.ACLExtension{wrapped: backend, policy: policy}
backend = &cache.RedisCacheExtension{wrapped: backend, redis: client}
vfs.Mount(path, backend)
```

### Checking Capabilities

```go
caps := backend.Capabilities()
for _, cap := range caps {
    if cap.Name == "Quota" {
        maxSize := cap.Params["maxSize"].(int64)
    }
}
```