package backend

type CapabilityType string

const (
	// Core capabilities by backend
	CapabilityMetadata      CapabilityType = "metadata"
	CapabilityObjectStorage CapabilityType = "object_storage"

	//
	CapabilityExtensionACL      CapabilityType = "acl"
	CapabilityExtensionCache    CapabilityType = "cache"
	CapabilityExtensionEncrypt  CapabilityType = "encrypt"
	CapabilityExtensionSnapshot CapabilityType = "snapshot"
	CapabilityExtensionRubbish  CapabilityType = "rubbish"
)

type Capability struct {
	Type   CapabilityType `json:"type"`
	Params map[string]any `json:"params,omitempty"`
}

type CapabilitySettings struct {
	IsReadonly    bool  `json:"isreadonly,omitempty"`
	MinObjectSize int64 `json:"min_object_size,omitempty"`
	MaxObjectSize int64 `json:"max_object_size,omitempty"`
}

type Capabilities struct {
	Capabilities []Capability       `json:"capabilities"`
	Settings     CapabilitySettings `json:"settings,omitempty"`
}

func GetFullCapabilities() *Capabilities {
	return &Capabilities{
		Capabilities: []Capability{
			{
				Type: CapabilityObjectStorage,
			},
			{
				Type: CapabilityExtensionACL,
			},
			{
				Type: CapabilityExtensionCache,
			},
			{
				Type: CapabilityExtensionEncrypt,
			},
			{
				Type: CapabilityExtensionSnapshot,
			},
			{
				Type: CapabilityExtensionRubbish,
			},
		},
		Settings: CapabilitySettings{
			IsReadonly:    false,
			MinObjectSize: 0,
			MaxObjectSize: 0,
		},
	}
}

// Contains checks if a capability is supported
func (c *Capabilities) Contains(cap CapabilityType) (bool, Capability) {
	for _, capability := range c.Capabilities {
		if capability.Type == cap {
			return true, capability
		}
	}

	return false, Capability{}
}
