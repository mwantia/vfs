package backend

// BackendCapability represents a capability that a backend can provide

import "slices"

type VirtualBackendCapability string

const (
	// Core capabilities by backend
	CapabilityMetadata      VirtualBackendCapability = "metadata"
	CapabilityObjectStorage VirtualBackendCapability = "object_storage"

	// Extension capabilities per 'metadata' or 'object-storage' backend
	CapabilityACL        VirtualBackendCapability = "acl"
	CapabilityCache      VirtualBackendCapability = "cache"
	CapabilityEncrypt    VirtualBackendCapability = "encrypt"
	CapabilitySnapshot   VirtualBackendCapability = "snapshot"
	CapabilityStreaming  VirtualBackendCapability = "streaming"
	CapabilityMultipart  VirtualBackendCapability = "multipart"
	CapabilityVersioning VirtualBackendCapability = "versioning"
	CapabilityRubbish    VirtualBackendCapability = "rubbish"
)

func GetAllCapabilities() *VirtualBackendCapabilities {
	return &VirtualBackendCapabilities{
		Capabilities: []VirtualBackendCapability{
			CapabilityMetadata,
			CapabilityObjectStorage,
			CapabilityACL,
			CapabilityCache,
			CapabilityEncrypt,
			CapabilitySnapshot,
			CapabilityStreaming,
			CapabilityMultipart,
			CapabilityVersioning,
			CapabilityRubbish,
		},
	}
}

// VirtualBackendCapabilities describes what a backend supports
type VirtualBackendCapabilities struct {
	Capabilities  []VirtualBackendCapability `json:"capabilities"`
	MinObjectSize int64                      `json:"min_object_size"`
	MaxObjectSize int64                      `json:"max_object_size"`
}

// Contains checks if a capability is supported
func (vbc *VirtualBackendCapabilities) Contains(cap VirtualBackendCapability) bool {
	return slices.Contains(vbc.Capabilities, cap)
}
