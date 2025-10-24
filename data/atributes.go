package data

import "time"

const (
	// Encoding (gzip, etc.)
	AttributeContentEncoding = "content-encoding"
	// Content checksum
	AttributeChecksum = "checksum"
	// Version number or string
	AttributeVersion = "version"
	// Access control list
	AttributeACL = "acl"
	// Whether content is encrypted
	AttributeEncrypted = "encrypted"
	// Target to simlink
	AttributeSymlinkTarget = "symlink-target"
)

// GetAttribute safely retrieves the attribute with a default value.
func (vfm *VirtualFileMetadata) GetAttribute(key string, defaultValue string) string {
	if vfm.Attributes == nil {
		return defaultValue
	}

	if value, exists := vfm.Attributes[key]; exists {
		return value
	}

	return defaultValue
}

// SetAttribute safely sets attribute, initializing the map if needed.
func (vfm *VirtualFileMetadata) SetAttribute(key, value string) {
	if vfm.Attributes == nil {
		vfm.Attributes = make(map[string]string)
	}

	vfm.Attributes[key] = value
	vfm.ModifyTime = time.Now()
}

// DeleteAttribute removes a attribute key.
func (vfm *VirtualFileMetadata) DeleteAttribute(key string) {
	if vfm.Attributes != nil {
		delete(vfm.Attributes, key)
		vfm.ModifyTime = time.Now()
	}
}

// HasAttribute checks if a attribute key exists.
func (vfm *VirtualFileMetadata) HasAttribute(key string) bool {
	if vfm.Attributes == nil {
		return false
	}

	_, exists := vfm.Attributes[key]
	return exists
}
