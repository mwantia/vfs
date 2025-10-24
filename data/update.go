package data

import (
	"maps"
	"time"
)

// VirtualVirtualFileMetadataUpdateMask controls which fields of an inode should be updated.
// This allows partial updates without needing to fetch and write back entire inodes.
type VirtualFileMetadataUpdateMask int

const (
	VirtualFileMetadataUpdateKey         VirtualFileMetadataUpdateMask = 1 << iota // Update Relative Path
	VirtualFileMetadataUpdateStorageHash                                           // Update Storage Hash
	VirtualFileMetadataUpdateStorageKey                                            // Update Storage Key
	VirtualFileMetadataUpdateType                                                  // Update File Type
	VirtualFileMetadataUpdateMode                                                  // Update File Mode (permissions)
	VirtualFileMetadataUpdateSize                                                  // Update Size
	VirtualFileMetadataUpdateUID                                                   // Update UID
	VirtualFileMetadataUpdateGID                                                   // Update GID
	VirtualFileMetadataUpdateAttributes                                            // Update Attributes map

	VirtualFileMetadataUpdateAll = ^VirtualFileMetadataUpdateMask(0) // Update all fields
)

// VirtualFileMetadataUpdate represents a partial update to an inode.
type VirtualFileMetadataUpdate struct {
	Mask     VirtualFileMetadataUpdateMask `json:"mask"`
	Metadata *VirtualFileMetadata          `json:"metadata"`
}

// Apply applies this update to an existing virtual file metadata.
func (vfmu *VirtualFileMetadataUpdate) Apply(target *VirtualFileMetadata) (bool, error) {
	// Use a dedicated value to check if any
	// modification to the target has been done
	modified := false

	if vfmu.Mask&VirtualFileMetadataUpdateKey != 0 {
		target.Key = vfmu.Metadata.Key
		modified = true
	}

	if vfmu.Mask&VirtualFileMetadataUpdateType != 0 {
		target.Type = vfmu.Metadata.Type
		modified = true
	}

	if vfmu.Mask&VirtualFileMetadataUpdateMode != 0 {
		target.Mode = vfmu.Metadata.Mode
		modified = true
	}

	if vfmu.Mask&VirtualFileMetadataUpdateSize != 0 {
		target.Size = vfmu.Metadata.Size
		modified = true
	}

	if vfmu.Mask&VirtualFileMetadataUpdateUID != 0 {
		target.UID = vfmu.Metadata.UID
		modified = true
	}
	if vfmu.Mask&VirtualFileMetadataUpdateGID != 0 {
		target.GID = vfmu.Metadata.GID
		modified = true
	}

	if vfmu.Mask&VirtualFileMetadataUpdateAttributes != 0 {
		target.Attributes = make(map[string]string, len(vfmu.Metadata.Attributes))
		maps.Copy(target.Attributes, vfmu.Metadata.Attributes)
		modified = true
	}

	// Only update ModifyTime if any form of modification actually happened
	if modified {
		target.ModifyTime = time.Now()
	}

	return modified, nil
}
