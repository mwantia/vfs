package builder

import (
	"fmt"

	"github.com/mwantia/vfs/mount/service"
)

func WithProfile(name string) MountStep {
	return func(mb *MountBuilder) error {
		profile, err := GetProfile(name)
		if err != nil {
			return fmt.Errorf("failed to get profile: %w", err)
		}

		steps := make([]MountStep, 0)
		if profile.ObjectStorage != "" {
			steps = append(steps, WithObjectStorage(profile.ObjectStorage))
		}
		// Add metadata (optional)
		if profile.Metadata != "" {
			steps = append(steps, WithMetadata(profile.Metadata))
		}
		// Add extensions (optional)
		for _, ext := range profile.Extensions {
			if ext.Uri != "" {
				steps = append(steps, WithExtension(ext.Type, ext.Uri))
			}
		}
		// Add options
		if profile.Options.Namespace != "" {
			steps = append(steps, WithNamespace(profile.Options.Namespace))
		}
		if profile.Options.PathPrefix != "" {
			steps = append(steps, WithPathPrefix(profile.Options.PathPrefix))
		}
		if profile.Options.IsReadOnly {
			steps = append(steps, AsReadOnly())
		}

		for _, step := range steps {
			if err := step(mb); err != nil {
				return fmt.Errorf("failed to complete build steps: %v", err)
			}
		}

		return nil
	}
}

// WithObjectStorage
func WithObjectStorage(uri string) MountStep {
	return func(mb *MountBuilder) error {
		if uri == "" {
			return fmt.Errorf("empty uri is not allowed")
		}

		mb.ObjectStorage = uri
		return nil
	}
}

// WithMetadata
func WithMetadata(uri string) MountStep {
	return func(mb *MountBuilder) error {
		if uri == "" {
			return fmt.Errorf("empty uri is not allowed")
		}

		mb.Metadata = uri
		return nil
	}
}

// WithExtension
func WithExtension(ext service.ServiceExtension, uri string) MountStep {
	return func(mb *MountBuilder) error {
		if uri == "" {
			return fmt.Errorf("empty uri is not allowed")
		}

		mb.Extensions[ext] = uri
		return nil
	}
}

// WithNamespace
func WithNamespace(namespace string) MountStep {
	return func(mb *MountBuilder) error {
		mb.Options.Namespace = namespace
		return nil
	}
}

// WithPathPrefix
func WithPathPrefix(pathPrefix string) MountStep {
	return func(mb *MountBuilder) error {
		mb.Options.PathPrefix = pathPrefix
		return nil
	}
}

// AsReadOnly specifies, if this mount is in a readonly state.
func AsReadOnly() MountStep {
	return func(mb *MountBuilder) error {
		mb.Options.IsReadOnly = true
		return nil
	}
}
