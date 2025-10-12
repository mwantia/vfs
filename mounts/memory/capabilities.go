package memory

import "github.com/mwantia/vfs"

// GetCapabilities returns the capabilities supported by this mount.
// MetaMemoryMount supports CRUD operations and metadata storage.
func (m *MetaMemoryMount) GetCapabilities() vfs.VirtualMountCapabilities {
	return vfs.VirtualMountCapabilities{
		Capabilities: []vfs.VirtualMountCapability{
			vfs.VirtualMountCapabilityCRUD,
			vfs.VirtualMountCapabilityMetadata,
			vfs.VirtualMountCapabilityStreaming,
		},
	}
}
