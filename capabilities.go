package vfs

import "slices"

// VirtualMountCapabilities describes the features supported by a mount implementation.
// Mount handlers declare their capabilities to allow clients to verify supported operations.
type VirtualMountCapabilities struct {
	Capabilities []VirtualMountCapability `json:"capabilities"`
}

// VirtualMountCapability represents a specific feature that a mount may support.
type VirtualMountCapability string

// Standard mount capabilities that implementations can declare.
const (
	VirtualMountCapabilityCRUD        VirtualMountCapability = "CRUD"        // Basic create, read, update, delete operations
	VirtualMountCapabilityEncrypt     VirtualMountCapability = "Encrypt"     // Transparent encryption/decryption
	VirtualMountCapabilityMetadata    VirtualMountCapability = "Metadata"    // Extended metadata storage
	VirtualMountCapabilityPermissions VirtualMountCapability = "Permissions" // Unix-style permissions enforcement
	VirtualMountCapabilitySnapshot    VirtualMountCapability = "Snapshot"    // Point-in-time snapshots
	VirtualMountCapabilityStreaming   VirtualMountCapability = "Streaming"   // Streaming I/O support
	VirtualMountCapabilityQuery       VirtualMountCapability = "Query"       // Advanced query operations
)

// ValidateCapability checks if a specific capability is supported by the mount.
// Returns true if the capability is present in the capabilities list.
func ValidateCapability(capabilities VirtualMountCapabilities, cap VirtualMountCapability) bool {
	return slices.Contains(capabilities.Capabilities, cap)
}
