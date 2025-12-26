package consul

import (
	"sync"

	"github.com/hashicorp/consul/api"
	"github.com/mwantia/vfs/mount/service"
)

var (
	DriverName = "consul"
	DriverType = service.ServiceTypeMonolith
)

// ConsulMonolithDriver
type ConsulMonolithDriver struct {
	mu     sync.RWMutex
	client *api.Client
	kv     *api.KV
	cfg    *ConsulBackendConfig
}

// ConsulObjectStorageService provides a simple object storage backend using HashiCorp Consul KV store.
//
// Architecture:
// - Objects are stored directly in Consul KV with their key as the path
// - Each object stores both content and minimal metadata (size, mode, timestamps) in a single KV entry
// - No separate metadata layer - everything is stored together
// - Prefix is configurable (default: "/")
//
// Limitations:
// - Consul KV has a 512KB limit per value
// - Best suited for configuration files, small assets, and metadata storage
type ConsulObjectStorageService struct {
	driver *ConsulMonolithDriver
}

// ConsulBackendConfig
type ConsulBackendConfig struct {
	// Address of the Consul server (default: "127.0.0.1:8500")
	Address string `json:"address"`

	// UseSSL enables HTTPS for Consul connection (default: false)
	UseSSL bool `json:"usessl"`

	// Token for Consul ACL authentication
	Token string `json:"token"`

	// Consul Datacenter to use
	Datacenter string `json:"datacenter"`

	// Namespace for Consul Enterprise
	Namespace string `json:"namespace"`

	// Prefix for all keys in Consul KV (default: "/")
	// This allows mounting the backend at a specific path
	Prefix string `json:"prefix"`
}
