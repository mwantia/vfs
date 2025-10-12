package vfs

import "slices"

// VirtualMountCapabilities describes the features supported by a mount implementation.
// Mount handlers declare their capabilities to allow clients to verify supported operations.
type VirtualMountCapabilities struct {
	Capabilities []VirtualMountCapability `json:"capabilities"`
}

// VirtualMountCapability represents a specific feature that a mount may support.
type VirtualMountCapability string

const (
	// Basic create, read, update, delete operations
	VirtualMountCapabilityCRUD VirtualMountCapability = "CRUD"
	// Transparent encryption/decryption
	VirtualMountCapabilityEncrypt VirtualMountCapability = "Encrypt"
	// Extended metadata storage
	VirtualMountCapabilityMetadata VirtualMountCapability = "Metadata"
	// Unix-style permissions enforcement
	VirtualMountCapabilityPermissions VirtualMountCapability = "Permissions"
	// Point-in-time snapshots
	VirtualMountCapabilitySnapshot VirtualMountCapability = "Snapshot"
	// Streaming I/O support
	VirtualMountCapabilityStreaming VirtualMountCapability = "Streaming"
	// Advanced query operations
	VirtualMountCapabilityQuery VirtualMountCapability = "Query"
)

// ValidateCapability checks if a specific capability is supported by the mount.
// Returns true if the capability is present in the capabilities list.
func ValidateCapability(capabilities VirtualMountCapabilities, cap VirtualMountCapability) bool {
	return slices.Contains(capabilities.Capabilities, cap)
}
