package mount

import (
	"fmt"

	"github.com/mwantia/vfs/mount/service"
)

type MountSpec struct {
	ObjectStorage string                              `json:"object_storage"`
	Metadata      string                              `json:"metadata"`
	Extensions    map[service.ServiceExtension]string `json:"extensions"`
	Options       *MountOptions                       `json:"options"`
}

// ToSteps converts a MountSpec to BuildSteps.
// This is used when restoring mounts from persisted specifications.
func (ms *MountSpec) ToSteps() ([]BuildStep, error) {
	steps := make([]BuildStep, 0)

	// Add object storage (required)
	if ms.ObjectStorage != "" {
		steps = append(steps, WithObjectStorage(ms.ObjectStorage))
	} else {
		return nil, fmt.Errorf("object storage is required")
	}

	// Add metadata (optional)
	if ms.Metadata != "" {
		steps = append(steps, WithMetadata(ms.Metadata))
	}

	// Add extensions (optional)
	for ext, uri := range ms.Extensions {
		if uri != "" {
			steps = append(steps, WithExtension(ext, uri))
		}
	}

	// Add options
	if ms.Options != nil {
		if ms.Options.Namespace != "" {
			steps = append(steps, WithNamespace(ms.Options.Namespace))
		}
		if ms.Options.PathPrefix != "" {
			steps = append(steps, WithPathPrefix(ms.Options.PathPrefix))
		}
		if ms.Options.IsReadOnly {
			steps = append(steps, AsReadOnly())
		}
	}

	return steps, nil
}
