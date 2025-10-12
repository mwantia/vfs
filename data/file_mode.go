package data

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
