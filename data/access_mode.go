package data

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
	AccessModeTrunc                                // O_TRUNC:  truncate on open
	AccessModeExcl                                 // O_EXCL:   exclusive creation (with CREATE)
	AccessModeSync                                 // O_SYNC:   synchronous I/O
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
