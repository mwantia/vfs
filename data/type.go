package data

// VirtualFileType identifies the type of object in the filesystem.
type VirtualFileType int

// File type constants matching common Unix file types.
const (
	FileTypeFile      VirtualFileType = iota // Regular file
	FileTypeMount                            // Mount file
	FileTypeDirectory                        // Directory
	FileTypeSymlink                          // Symbolic link
	FileTypeDevice                           // Device file
	FileTypeSocket                           // Unix socket
)
