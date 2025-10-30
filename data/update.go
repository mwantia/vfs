package data

import (
	"maps"
	"time"
)

// MetadataUpdateMask controls which fields of an inode should be updated.
// This allows partial updates without needing to fetch and write back entire inodes.
type MetadataUpdateMask int

const (
	MetadataUpdateKey         MetadataUpdateMask = 1 << iota // Update Relative Path
	MetadataUpdateStorageHash                                // Update Storage Hash
	MetadataUpdateStorageKey                                 // Update Storage Key
	MetadataUpdateMode                                       // Update File Mode (permissions)
	MetadataUpdateSize                                       // Update Size
	MetadataUpdateUID                                        // Update UID
	MetadataUpdateGID                                        // Update GID
	MetadataUpdateAttributes                                 // Update Attributes map

	MetadataUpdateAll = ^MetadataUpdateMask(0) // Update all fields
)

// MetadataUpdate represents a partial update to an inode.
type MetadataUpdate struct {
	Mask     MetadataUpdateMask `json:"mask"`
	Metadata *Metadata          `json:"metadata"`
}

// Apply applies this update to an existing virtual file metadata.
func (mu *MetadataUpdate) Apply(target *Metadata) (bool, error) {
	// Use a dedicated value to check if any
	// modification to the target has been done
	modified := false

	if mu.Mask&MetadataUpdateKey != 0 {
		target.Key = mu.Metadata.Key
		modified = true
	}

	if mu.Mask&MetadataUpdateMode != 0 {
		target.Mode = mu.Metadata.Mode
		modified = true
	}

	if mu.Mask&MetadataUpdateSize != 0 {
		target.Size = mu.Metadata.Size
		modified = true
	}

	if mu.Mask&MetadataUpdateUID != 0 {
		target.UID = mu.Metadata.UID
		modified = true
	}
	if mu.Mask&MetadataUpdateGID != 0 {
		target.GID = mu.Metadata.GID
		modified = true
	}

	if mu.Mask&MetadataUpdateAttributes != 0 {
		target.Attributes = make(map[string]string, len(mu.Metadata.Attributes))
		maps.Copy(target.Attributes, mu.Metadata.Attributes)
		modified = true
	}

	// Only update ModifyTime if any form of modification actually happened
	if modified {
		target.ModifyTime = time.Now()
	}

	return modified, nil
}
