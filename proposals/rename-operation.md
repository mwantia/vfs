# Proposal: Rename Operation Implementation

## Overview

This proposal outlines different approaches for implementing the `Rename` operation in the VFS, considering efficiency, atomicity, and the existing CRUD-based architecture.

## Background

The VFS currently uses a minimal 7-method interface for mounts (CRUD + List + Stat + Truncate). The `Rename` operation at `vfs.go:276` is currently unimplemented and needs a design decision.

## Key Considerations

### 1. Same-Mount vs Cross-Mount Operations
- **Same mount**: Can potentially be optimized with native backend support (e.g., OS-level rename, database UPDATE)
- **Cross-mount**: Must use copy + delete approach (or disallow entirely)

### 2. File vs Directory Handling
- **Files**: Simple copy + delete
- **Directories**: Recursive operation - potentially expensive without native support

### 3. Atomicity
- Native rename operations are typically atomic
- CRUD-based approach is NOT atomic (failure mid-operation leaves inconsistent state)

### 4. Open File Handles
- Files currently open (in `vfs.streams`) should be handled:
  - Block rename if file is open?
  - Update stream paths after rename?
  - Close streams before rename?

## Proposed Approaches

### Approach 1: Add Rename to VirtualMount Interface

Add `Rename` as a required method on the `VirtualMount` interface.

**Interface Addition:**
```go
// VirtualMount interface
Rename(ctx context.Context, oldPath string, newPath string) error
```

**Pros:**
- Allows mounts to implement efficient, atomic renames
- MemoryMount can do O(1) map key rename
- LocalMount can use `os.Rename()` (atomic on same filesystem)
- Each mount controls its own rename semantics

**Cons:**
- Increases interface size (violates minimal interface philosophy)
- All mounts must implement it (even if just CRUD internally)

**Implementation Complexity:** Low - each mount implements as needed

---

### Approach 2: Optional Rename Capability

Add `Rename` to the interface but make it optional via capabilities.

**Interface Addition:**
```go
// VirtualMount interface
Rename(ctx context.Context, oldPath string, newPath string) error

// VirtualMountCapability
VirtualMountCapabilityRename VirtualMountCapability = "rename"
```

**VFS Logic:**
```go
func (vfs *VirtualFileSystem) Rename(ctx context.Context, oldPath, newPath string) error {
    // Check if mount supports rename capability
    if entry.mount.GetCapabilities().Has(VirtualMountCapabilityRename) {
        return entry.mount.Rename(ctx, relOldPath, relNewPath)
    }

    // Fall back to CRUD implementation
    return vfs.renameCRUD(ctx, entry, relOldPath, relNewPath)
}
```

**Pros:**
- Best of both worlds: efficiency where possible, fallback where needed
- Mounts advertise their capabilities clearly
- VFS handles fallback logic centrally

**Cons:**
- More complex implementation
- Capability checking overhead
- Need to maintain CRUD fallback code

**Implementation Complexity:** Medium - requires capability check + fallback implementation

---

### Approach 3: VFS-Only Implementation (Pure CRUD)

Implement `Rename` entirely in VFS layer using existing mount methods.

**VFS Implementation:**
```go
func (vfs *VirtualFileSystem) Rename(ctx context.Context, oldPath, newPath string) error {
    // 1. Verify same mount
    // 2. Check old path exists, new path doesn't
    // 3. For files: Read → Create → Write → Delete
    // 4. For directories: Recursive copy + delete
    // 5. Handle cleanup on failure
}
```

**Pros:**
- No interface changes
- Maintains minimal mount interface
- Works across all mounts uniformly

**Cons:**
- Not atomic (failure mid-operation is problematic)
- Inefficient for large directories
- No way to leverage backend-native rename
- Memory intensive for large files

**Implementation Complexity:** Medium - need robust error handling and cleanup

---

### Approach 4: Hybrid - VFS with Mount Helper

Add a **helper method** (not interface requirement) that mounts can optionally implement.

**Pattern:**
```go
// Optional interface that mounts can implement
type RenameableMount interface {
    VirtualMount
    Rename(ctx context.Context, oldPath string, newPath string) error
}

// VFS checks with type assertion
func (vfs *VirtualFileSystem) Rename(ctx context.Context, oldPath, newPath string) error {
    if rm, ok := entry.mount.(RenameableMount); ok {
        return rm.Rename(ctx, relOldPath, relNewPath)
    }

    // Fall back to CRUD
    return vfs.renameCRUD(ctx, entry, relOldPath, relNewPath)
}
```

**Pros:**
- Interface stays minimal (optional extension)
- Type assertion is idiomatic Go
- Efficient where supported, works everywhere

**Cons:**
- Less explicit than capabilities
- Harder to discover what mounts support rename

**Implementation Complexity:** Medium - type assertion + fallback

---

## Special Cases to Handle

### 1. Cross-Mount Rename
**Options:**
- **Disallow entirely** (return error) - simplest, matches Unix semantics
- **Allow with warning** (expensive copy operation)
- **Require explicit flag** to allow cross-mount

**Recommendation:** Disallow (return `ErrCrossMount`)

### 2. Open File Streams
**Options:**
- **Block rename** if file is in `vfs.streams` (return `ErrBusy`)
- **Force close** streams before rename
- **Update stream paths** after rename (complex)

**Recommendation:** Block rename if file is open (safest, simplest)

### 3. Read-Only Mounts
**Handling:**
- Check `entry.options.ReadOnly` before operation
- Return `ErrReadOnly` if mount is read-only

### 4. Parent Directory Existence
**Handling:**
- Verify parent of `newPath` exists before operation
- Return `ErrNotExist` if parent missing

### 5. Overwrite Behavior
**Options:**
- **Never overwrite** - return `ErrExist` if newPath exists
- **Optional overwrite** - flag parameter to allow
- **Always overwrite** - like Unix `mv`

**Recommendation:** Never overwrite (require explicit delete first)

---

## Recommendations

### Short Term (MVP)
Use **Approach 3** (VFS-Only CRUD):
- Implement basic rename in VFS using existing methods
- Disallow cross-mount renames
- Block rename if file is open
- No overwrite support

**Rationale:**
- Fastest to implement
- No interface changes during early development
- Works for basic use cases

### Long Term (Production)
Migrate to **Approach 2** (Optional Capability):
- Add rename capability flag
- Let mounts implement native rename when possible
- Keep CRUD fallback for mounts without native support

**Rationale:**
- Performance matters for production
- Capability system already exists
- Atomic operations are important for data safety

---

## Implementation Checklist

### VFS Layer (`vfs.go:276`)
- [ ] Verify both paths resolve to same mount
- [ ] Check if source exists (`Stat`)
- [ ] Check if destination exists (return `ErrExist`)
- [ ] Verify parent of destination exists
- [ ] Check for open streams on source path
- [ ] Check read-only mount status
- [ ] Handle file rename (read + create + write + delete)
- [ ] Handle directory rename (recursive copy + delete)
- [ ] Implement cleanup on failure (rollback partial operations)
- [ ] Add error for cross-mount attempts

### Mount Implementation (if adding to interface)
- [ ] Add `Rename` to `VirtualMount` interface
- [ ] Implement in `MemoryMount` (map key rename)
- [ ] Stub in `LocalMount` (placeholder for future)
- [ ] Handle in `ReadOnlyMount` (return `ErrReadOnly`)

### Testing (`tests/`)
- [ ] Test same-directory rename (file)
- [ ] Test cross-directory rename (file)
- [ ] Test directory rename (empty)
- [ ] Test directory rename (with contents)
- [ ] Test rename with open file handle (should fail)
- [ ] Test cross-mount rename (should fail)
- [ ] Test rename on read-only mount (should fail)
- [ ] Test rename when destination exists (should fail)
- [ ] Test rename with missing parent directory (should fail)

---

## Open Questions

1. **Should we support renaming mount points?**
   - Currently no way to rename a mount path after mounting
   - Would require updating `vfs.mounts` map and all child mounts

2. **Should rename preserve metadata?**
   - Modification time: update to now, or preserve from source?
   - Custom metadata: currently only MemoryMount supports this

3. **How to handle case-only renames on case-insensitive backends?**
   - e.g., `file.txt` → `File.txt` on Windows/macOS
   - May need special handling to avoid "file already exists" errors

4. **Should there be a rename size limit?**
   - To prevent OOM on huge directory renames
   - Could add context cancellation support

---

## Related Files
- `vfs.go:276` - Current stub implementation
- `mount.go:12-61` - VirtualMount interface definition
- `capabilities.go` - Mount capability system
- `errors.go` - Error definitions (may need `ErrCrossMount`)
- `mounts/memory.go` - Reference implementation
- `tests/` - Test coverage

---

## References
- Unix `rename(2)` syscall semantics
- Go `os.Rename()` behavior
- POSIX filesystem semantics for rename operations
