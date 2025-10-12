package data

import "time"

const (
	// MIME type
	MetadataContentType = "content-type"
	// Encoding (gzip, etc.)
	MetadataContentEncoding = "content-encoding"
	// Content checksum
	MetadataChecksum = "checksum"
	// Version number or string
	MetadataVersion = "version"
	// Access control list
	MetadataACL = "acl"
	// Whether content is encrypted
	MetadataEncrypted = "encrypted"
)

// GetMetadata safely retrieves metadata with a default value.
func (vi *VirtualInode) GetMetadata(key string, defaultValue string) string {
	if vi.Metadata == nil {
		return defaultValue
	}

	if value, exists := vi.Metadata[key]; exists {
		return value
	}

	return defaultValue
}

// SetMetadata safely sets metadata, initializing the map if needed.
func (vi *VirtualInode) SetMetadata(key, value string) {
	if vi.Metadata == nil {
		vi.Metadata = make(map[string]string)
	}

	vi.Metadata[key] = value
	vi.ChangeTime = time.Now()
}

// DeleteMetadata removes a metadata key.
func (vi *VirtualInode) DeleteMetadata(key string) {
	if vi.Metadata != nil {
		delete(vi.Metadata, key)
		vi.ChangeTime = time.Now()
	}
}

// HasMetadata checks if a metadata key exists.
func (vi *VirtualInode) HasMetadata(key string) bool {
	if vi.Metadata == nil {
		return false
	}

	_, exists := vi.Metadata[key]
	return exists
}
