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
func (m *Metadata) GetAttribute(key string, defaultValue string) string {
	if m.Attributes == nil {
		return defaultValue
	}

	if value, exists := m.Attributes[key]; exists {
		return value
	}

	return defaultValue
}

// SetAttribute safely sets attribute, initializing the map if needed.
func (m *Metadata) SetAttribute(key, value string) {
	if m.Attributes == nil {
		m.Attributes = make(map[string]string)
	}

	m.Attributes[key] = value
	m.ModifyTime = time.Now()
}

// DeleteAttribute removes a attribute key.
func (m *Metadata) DeleteAttribute(key string) {
	if m.Attributes != nil {
		delete(m.Attributes, key)
		m.ModifyTime = time.Now()
	}
}

// HasAttribute checks if a attribute key exists.
func (m *Metadata) HasAttribute(key string) bool {
	if m.Attributes == nil {
		return false
	}

	_, exists := m.Attributes[key]
	return exists
}
