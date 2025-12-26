package mount

import (
	"fmt"
	"sync"

	"github.com/mwantia/vfs/mount/service"
)

type MountExtensionSpec struct {
	Type   service.ServiceExtension `json:"type"`
	Uri    string                   `json:"uri"`
	Config map[string]any           `json:"config"`
}

type MountProfileOptions struct {
	Namespace  string `json:"namespace"`
	PathPrefix string `json:"path_prefix"`
	IsReadOnly bool   `json:"is_readonly"`
}

type MountProfile struct {
	Name          string               `json:"name"`
	Description   string               `json:"description"`
	ObjectStorage string               `json:"object_storage"`
	Metadata      string               `json:"metadata"`
	Extensions    []MountExtensionSpec `json:"extensions"`
	Options       MountProfileOptions  `json:"options"`
}

func (mp *MountProfile) ToSteps() ([]BuildStep, error) {
	steps := make([]BuildStep, 0)

	if mp.ObjectStorage != "" {
		steps = append(steps, WithObjectStorage(mp.ObjectStorage))
	}
	if mp.Metadata != "" {
		steps = append(steps, WithMetadata(mp.Metadata))
	}

	if mp.Options.Namespace != "" {
		steps = append(steps, WithNamespace(mp.Options.Namespace))
	}
	if mp.Options.PathPrefix != "" {
		steps = append(steps, WithPathPrefix(mp.Options.PathPrefix))
	}
	if mp.Options.IsReadOnly {
		steps = append(steps, AsReadOnly())
	}

	for _, extension := range mp.Extensions {
		if extension.Uri != "" {
			steps = append(steps, WithExtension(extension.Type, extension.Uri))
		}
	}

	return steps, nil
}

type MountProfileRegistry struct {
	mu       sync.RWMutex
	profiles map[string]*MountProfile
}

var globalProfiles = &MountProfileRegistry{
	profiles: map[string]*MountProfile{
		"ephemeral": {
			Name:          "Ephemeral",
			Description:   "Ephemeral in-memory storage",
			ObjectStorage: "ephemeral://",
			Options: MountProfileOptions{
				Namespace:  "",
				PathPrefix: "",
				IsReadOnly: false,
			},
		},
	},
}

func RegisterProfile(profile *MountProfile) {
	globalProfiles.mu.Lock()
	defer globalProfiles.mu.Unlock()
	globalProfiles.profiles[profile.Name] = profile
}

func GetProfile(name string) (*MountProfile, error) {
	globalProfiles.mu.RLock()
	defer globalProfiles.mu.RUnlock()

	profile, exists := globalProfiles.profiles[name]
	if !exists {
		return nil, fmt.Errorf("profile not found")
	}
	return profile, nil
}
