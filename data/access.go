package data

// AccessMode represents file access modes for opening files.
// These modes control how files are opened (read, write, append, etc.).
type AccessMode int

// File access mode constants.
// These can be combined using bitwise OR.
const (
	AccessModeRead   AccessMode = 1 << iota // O_RDONLY: open for reading
	AccessModeWrite                         // O_WRONLY: open for writing
	AccessModeAppend                        // O_APPEND: append to file
	AccessModeCreate                        // O_CREATE: create if not exists
	AccessModeTrunc                         // O_TRUNC:  truncate on open
	AccessModeExcl                          // O_EXCL:   exclusive creation (with CREATE)
	AccessModeSync                          // O_SYNC:   synchronous I/O
)

// IsReadOnly checks if the mode only allows reading.
func (m AccessMode) IsReadOnly() bool {
	return m&AccessModeRead != 0 && m&AccessModeWrite == 0
}

// IsWriteOnly checks if the mode only allows writing.
func (m AccessMode) IsWriteOnly() bool {
	return m&AccessModeWrite != 0 && m&AccessModeRead == 0
}

// IsReadWrite checks if the mode allows both reading and writing.
func (m AccessMode) IsReadWrite() bool {
	return m&AccessModeRead != 0 && m&AccessModeWrite != 0
}

// HasAppend checks if the mode includes append.
func (m AccessMode) HasAppend() bool {
	return m&AccessModeAppend != 0
}

// HasCreate checks if the mode includes create.
func (m AccessMode) HasCreate() bool {
	return m&AccessModeCreate != 0
}

// HasTrunc checks if the mode includes truncate.
func (m AccessMode) HasTrunc() bool {
	return m&AccessModeTrunc != 0
}

// HasExcl checks if the mode includes exclusive creation.
func (m AccessMode) HasExcl() bool {
	return m&AccessModeExcl != 0
}

// HasSync checks if the mode includes synchronous I/O.
func (m AccessMode) HasSync() bool {
	return m&AccessModeSync != 0
}
