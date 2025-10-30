package backend

// BackendCapability represents a capability that a backend can provide

import "slices"

type BackendCapability string

const (
	// Core capabilities by backend
	CapabilityMetadata      BackendCapability = "metadata"
	CapabilityObjectStorage BackendCapability = "object_storage"

	// Extension capabilities per 'metadata' or 'object-storage' backend
	CapabilityACL        BackendCapability = "acl"
	CapabilityCache      BackendCapability = "cache"
	CapabilityEncrypt    BackendCapability = "encrypt"
	CapabilityNamespace  BackendCapability = "namespace"
	CapabilitySnapshot   BackendCapability = "snapshot"
	CapabilityStreaming  BackendCapability = "streaming"
	CapabilityMultipart  BackendCapability = "multipart"
	CapabilityVersioning BackendCapability = "versioning"
	CapabilityRubbish    BackendCapability = "rubbish"
)

func GetAllCapabilities() *BackendCapabilities {
	return &BackendCapabilities{
		Capabilities: []BackendCapability{
			CapabilityMetadata,
			CapabilityObjectStorage,
			CapabilityACL,
			CapabilityCache,
			CapabilityEncrypt,
			CapabilityNamespace,
			CapabilitySnapshot,
			CapabilityStreaming,
			CapabilityMultipart,
			CapabilityVersioning,
			CapabilityRubbish,
		},
	}
}

// BackendCapabilities describes what a backend supports
type BackendCapabilities struct {
	Capabilities  []BackendCapability `json:"capabilities"`
	MinObjectSize int64               `json:"min_object_size"`
	MaxObjectSize int64               `json:"max_object_size"`
}

// Contains checks if a capability is supported
func (vbc *BackendCapabilities) Contains(cap BackendCapability) bool {
	return slices.Contains(vbc.Capabilities, cap)
}
