package builder

import (
	"fmt"
	"sync"

	"github.com/mwantia/vfs/mount/service"
)

type MountProfile struct {
	Name          string                         `json:"name"`
	Description   string                         `json:"description"`
	ObjectStorage string                         `json:"object_storage"`
	Metadata      string                         `json:"metadata"`
	Extensions    []MountExtensionProfileOptions `json:"extensions"`
	Options       MountProfileOptions            `json:"options"`
}

type MountProfileOptions struct {
	Namespace  string `json:"namespace"`
	PathPrefix string `json:"path_prefix"`
	IsReadOnly bool   `json:"is_readonly"`
}

type MountExtensionProfileOptions struct {
	Type service.ServiceExtension `json:"type"`
	Uri  string                   `json:"uri"`
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
