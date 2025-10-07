package vfs

import "time"

// VirtualFileInfo describes a file or directory in the VFS.
// It provides metadata similar to os.FileInfo but tailored for virtual filesystems.
type VirtualFileInfo struct {
	Name    string          // Base name of the file
	Path    string          // Full name path of the file
	Size    int64           // Length in bytes for regular files
	Mode    VirtualFileMode // File mode bits
	ModTime time.Time       // Modification time
	IsDir   bool            // Abbreviation for Mode().IsDir()
}

// VirtualFileMode represents file mode and permission bits.
// It follows Unix file mode conventions with type and permission bits.
type VirtualFileMode uint32

// File mode constants for type and permission bits.
// These match Unix file mode semantics.
const (
	// Type bits
	ModeDir        VirtualFileMode = 1 << 31 // d: directory
	ModeSymlink    VirtualFileMode = 1 << 30 // L: symbolic link
	ModeNamedPipe  VirtualFileMode = 1 << 29 // p: named pipe (FIFO)
	ModeSocket     VirtualFileMode = 1 << 28 // S: Unix domain socket
	ModeDevice     VirtualFileMode = 1 << 27 // D: device file
	ModeCharDevice VirtualFileMode = 1 << 26 // c: Unix character device
	ModeIrregular  VirtualFileMode = 1 << 25 // ?: non-regular file

	// Permission bits
	ModePerm VirtualFileMode = 0777 // Unix permission bits
)

// IsDir reports whether m describes a directory.
// It checks if the ModeDir bit is set.
func (m VirtualFileMode) IsDir() bool {
	return m&ModeDir != 0
}

// IsRegular reports whether m describes a regular file.
// A regular file has no type bits set (not directory, symlink, device, etc.).
func (m VirtualFileMode) IsRegular() bool {
	return m&(ModeDir|ModeSymlink|ModeNamedPipe|ModeSocket|ModeDevice|ModeCharDevice|ModeIrregular) == 0
}

// Perm returns the Unix permission bits in m (the lower 9 bits).
func (m VirtualFileMode) Perm() VirtualFileMode {
	return m & ModePerm
}

// String returns a textual representation of the mode in Unix ls -l format.
// Example: "drwxr-xr-x" for a directory with 755 permissions.
func (m VirtualFileMode) String() string {
	const str = "dalTLDpSugct?"
	var buf [32]byte
	w := 0

	// Type bits
	for i, c := range str {
		if m&(1<<uint(32-1-i)) != 0 {
			buf[w] = byte(c)
			w++
		}
	}

	if w == 0 {
		buf[w] = '-'
		w++
	}

	// Permission bits
	const rwx = "rwxrwxrwx"
	for i, c := range rwx {
		if m&(1<<uint(9-1-i)) != 0 {
			buf[w] = byte(c)
		} else {
			buf[w] = '-'
		}
		w++
	}

	return string(buf[:w])
}

// VirtualAccessMode represents file access modes for opening files.
// These modes control how files are opened (read, write, append, etc.).
type VirtualAccessMode int

// File access mode constants.
// These can be combined using bitwise OR.
const (
	AccessModeRead   VirtualAccessMode = 1 << iota // O_RDONLY: open for reading
	AccessModeWrite                                // O_WRONLY: open for writing
	AccessModeAppend                               // O_APPEND: append to file
	AccessModeCreate                               // O_CREATE: create if not exists
	AccessModeTrunc                                // O_TRUNC: truncate on open
	AccessModeExcl                                 // O_EXCL: exclusive creation (with CREATE)
	AccessModeSync                                 // O_SYNC: synchronous I/O
)

// IsReadOnly checks if the mode only allows reading.
func (m VirtualAccessMode) IsReadOnly() bool {
	return m&AccessModeRead != 0 && m&AccessModeWrite == 0
}

// IsWriteOnly checks if the mode only allows writing.
func (m VirtualAccessMode) IsWriteOnly() bool {
	return m&AccessModeWrite != 0 && m&AccessModeRead == 0
}

// IsReadWrite checks if the mode allows both reading and writing.
func (m VirtualAccessMode) IsReadWrite() bool {
	return m&AccessModeRead != 0 && m&AccessModeWrite != 0
}

// HasAppend checks if the mode includes append.
func (m VirtualAccessMode) HasAppend() bool {
	return m&AccessModeAppend != 0
}

// HasCreate checks if the mode includes create.
func (m VirtualAccessMode) HasCreate() bool {
	return m&AccessModeCreate != 0
}

// HasTrunc checks if the mode includes truncate.
func (m VirtualAccessMode) HasTrunc() bool {
	return m&AccessModeTrunc != 0
}

// HasExcl checks if the mode includes exclusive creation.
func (m VirtualAccessMode) HasExcl() bool {
	return m&AccessModeExcl != 0
}

// HasSync checks if the mode includes synchronous I/O.
func (m VirtualAccessMode) HasSync() bool {
	return m&AccessModeSync != 0
}
