package builder

import (
	"fmt"
	"maps"

	"github.com/mwantia/vfs/mount/service"
)

type MountBuilder struct {
	ObjectStorage string                              `json:"object_storage"`
	Metadata      string                              `json:"metadata"`
	Extensions    map[service.ServiceExtension]string `json:"extensions"`
	Options       MountOptions                        `json:"options"`
}

type MountStep func(*MountBuilder) error

func BuildMounter(steps ...MountStep) (*MountBuilder, error) {
	builder := &MountBuilder{
		Options: MountOptions{
			// Default value options
			Namespace:  "",
			PathPrefix: "",
			Cascading:  false,
			IsReadOnly: false,
		},
		Extensions: make(map[service.ServiceExtension]string),
	}

	for _, step := range steps {
		if err := step(builder); err != nil {
			return nil, fmt.Errorf("failed to complete build step: %v", err)
		}
	}

	return builder, nil
}

// ToMountSpec converts a MountBuilder into a MountSpecifications.
// This is used when persisting mount configurations.
func (mb *MountBuilder) ToMountSpec() MountSpecifications {
	spec := MountSpecifications{
		ObjectStorage: mb.ObjectStorage,
		Metadata:      mb.Metadata,
		Extensions:    make(map[service.ServiceExtension]string),
		Options: &MountOptions{
			Namespace:  mb.Options.Namespace,
			PathPrefix: mb.Options.PathPrefix,
			IsReadOnly: mb.Options.IsReadOnly,
		},
	}
	// Convert extensions map
	maps.Copy(spec.Extensions, mb.Extensions)

	return spec
}
