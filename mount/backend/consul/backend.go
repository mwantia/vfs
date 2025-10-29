package consul

import (
	"context"
	"sync"

	"github.com/hashicorp/consul/api"
	"github.com/mwantia/vfs/mount/backend"
)

// ConsulBackend provides a simple object storage backend using HashiCorp Consul KV store.
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
type ConsulBackend struct {
	mu     sync.RWMutex
	client *api.Client
	kv     *api.KV

	// Configuration
	config *ConsulBackendConfig
}

// ConsulBackendConfig contains configuration options for the Consul backend
type ConsulBackendConfig struct {
	// Address of the Consul server (default: "127.0.0.1:8500")
	Address string

	// Token for Consul ACL authentication (optional)
	Token string

	// Datacenter to use (optional)
	Datacenter string

	// Namespace for Consul Enterprise (optional)
	Namespace string

	// Prefix for all keys in Consul KV (default: "/")
	// This allows mounting the backend at a specific path
	Prefix string
}

// NewConsulBackend creates a new Consul-backed object storage backend
func NewConsulBackend(config *ConsulBackendConfig) (*ConsulBackend, error) {
	if config == nil {
		config = &ConsulBackendConfig{}
	}

	// Set defaults
	if config.Address == "" {
		config.Address = "127.0.0.1:8500"
	}

	if config.Prefix == "" {
		config.Prefix = "/"
	}

	// Create Consul client
	clientConfig := api.DefaultConfig()
	clientConfig.Address = config.Address
	if config.Token != "" {
		clientConfig.Token = config.Token
	}
	if config.Datacenter != "" {
		clientConfig.Datacenter = config.Datacenter
	}
	if config.Namespace != "" {
		clientConfig.Namespace = config.Namespace
	}

	client, err := api.NewClient(clientConfig)
	if err != nil {
		return nil, err
	}

	backend := &ConsulBackend{
		client: client,
		kv:     client.KV(),
		config: config,
	}

	return backend, nil
}

// Name returns the identifier name defined for this backend
func (*ConsulBackend) Name() string {
	return "consul"
}

// Open is part of the lifecycle behaviour and gets called when opening this backend
func (cb *ConsulBackend) Open(ctx context.Context) error {
	// Nothing to initialize - Consul handles connections
	return nil
}

// Close is part of the lifecycle behaviour and gets called when closing this backend
func (cb *ConsulBackend) Close(ctx context.Context) error {
	// Nothing to clean up - Consul client is stateless
	return nil
}

// GetCapabilities returns a list of capabilities supported by this backend
func (cb *ConsulBackend) GetCapabilities() *backend.VirtualBackendCapabilities {
	return &backend.VirtualBackendCapabilities{
		Capabilities: []backend.VirtualBackendCapability{
			backend.CapabilityObjectStorage,
		},
		// Consul KV has a default limit of 512KB per value
		// We set it slightly lower to account for metadata overhead
		MaxObjectSize: 500 * 1024, // 500 KB
	}
}

// buildKey constructs the full Consul KV key from the object key
func (cb *ConsulBackend) buildKey(key string) string {
	// Remove leading / from key if present
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// Handle "/" prefix specially - it means no prefix, just use the key
	if cb.config.Prefix == "/" {
		return key
	}

	// For other prefixes, ensure they end with /
	prefix := cb.config.Prefix
	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return prefix + key
}
