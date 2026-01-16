package builder

// MountSpecUpdateMask controls which fields of a MountSpecifications should be updated.
// This allows partial updates without needing to fetch and write back entire specs.
type MountSpecUpdateMask int

const (
	MountSpecUpdateObjectStorage MountSpecUpdateMask = 1 << iota // Update ObjectStorage URI
	MountSpecUpdateMetadata                                      // Update Metadata URI
	MountSpecUpdateNamespace                                     // Update Options.Namespace
	MountSpecUpdatePathPrefix                                    // Update Options.PathPrefix
	MountSpecUpdateCascading                                     // Update Options.Cascading
	MountSpecUpdateReadOnly                                      // Update Options.IsReadOnly

	MountSpecUpdateAll = ^MountSpecUpdateMask(0) // Update all fields
)

// MountSpecUpdate represents a partial update to a MountSpecifications.
type MountSpecUpdate struct {
	Mask MountSpecUpdateMask  `json:"mask"`
	Spec MountSpecifications `json:"spec"`
}

// Apply applies this update to an existing MountSpecifications.
// Returns true if any modification was made.
func (u MountSpecUpdate) Apply(target *MountSpecifications) bool {
	modified := false

	if u.Mask&MountSpecUpdateObjectStorage != 0 {
		target.ObjectStorage = u.Spec.ObjectStorage
		modified = true
	}

	if u.Mask&MountSpecUpdateMetadata != 0 {
		target.Metadata = u.Spec.Metadata
		modified = true
	}

	// Ensure Options exists before updating its fields
	if u.Mask&(MountSpecUpdateNamespace|MountSpecUpdatePathPrefix|MountSpecUpdateCascading|MountSpecUpdateReadOnly) != 0 {
		if target.Options == nil {
			target.Options = &MountOptions{}
		}
		if u.Spec.Options == nil {
			u.Spec.Options = &MountOptions{}
		}
	}

	if u.Mask&MountSpecUpdateNamespace != 0 {
		target.Options.Namespace = u.Spec.Options.Namespace
		modified = true
	}

	if u.Mask&MountSpecUpdatePathPrefix != 0 {
		target.Options.PathPrefix = u.Spec.Options.PathPrefix
		modified = true
	}

	if u.Mask&MountSpecUpdateCascading != 0 {
		target.Options.Cascading = u.Spec.Options.Cascading
		modified = true
	}

	if u.Mask&MountSpecUpdateReadOnly != 0 {
		target.Options.IsReadOnly = u.Spec.Options.IsReadOnly
		modified = true
	}

	return modified
}
