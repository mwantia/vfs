package vfs

import "slices"

type VirtualMountCapabilities struct {
	Capabilities []VirtualMountCapability `json:"capabilities"`
}

type VirtualMountCapability string

const (
	VirtualMountCapabilityCRUD        VirtualMountCapability = "CRUD"
	VirtualMountCapabilityEncrypt     VirtualMountCapability = "Encrypt"
	VirtualMountCapabilityMetadata    VirtualMountCapability = "Metadata"
	VirtualMountCapabilityPermissions VirtualMountCapability = "Permissions"
	VirtualMountCapabilitySnapshot    VirtualMountCapability = "Snapshot"
	VirtualMountCapabilityStreaming   VirtualMountCapability = "Streaming"
	VirtualMountCapabilityQuery       VirtualMountCapability = "Query"
)

func ValidateCapability(capabilities VirtualMountCapabilities, cap VirtualMountCapability) bool {
	return slices.Contains(capabilities.Capabilities, cap)
}
