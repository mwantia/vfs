package consul

import (
	"context"
	"fmt"

	"github.com/hashicorp/consul/api"
	"github.com/mwantia/vfs/mount/service"
)

func NewConsulMonolithDriver(uri *service.Uri, ssl bool) (*ConsulMonolithDriver, error) {
	cfg := parseConsulConfig(uri, ssl)
	config := api.DefaultConfig()
	config.Address = cfg.Address
	if cfg.Token != "" {
		config.Token = cfg.Token
	}
	if cfg.Datacenter != "" {
		config.Datacenter = cfg.Datacenter
	}
	if cfg.Namespace != "" {
		config.Namespace = cfg.Namespace
	}

	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul client: %v", err)
	}

	return &ConsulMonolithDriver{
		client: client,
		kv:     client.KV(),
		cfg:    cfg,
	}, nil
}

// Name
func (*ConsulMonolithDriver) Name() string {
	return DriverName
}

// Health returns the health status of the driver by checking Consul cluster health
func (d *ConsulMonolithDriver) Health() (bool, error) {
	// Check if Consul cluster has a leader - this indicates the cluster is operational
	// This is a lightweight check that verifies connectivity and cluster health
	leader, err := d.client.Status().Leader()
	if err != nil {
		return false, err
	}
	// If there's no leader, the cluster is not healthy
	if leader == "" {
		return false, fmt.Errorf("no known cluster leader")
	}
	return true, nil
}

// IsBusy checks if the driver is currently busy with operations
func (d *ConsulMonolithDriver) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the driver is busy
	if !d.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	d.mu.Unlock()
	return false
}

// GetCapabilities returns a list of capabilities supported by this backend.
func (*ConsulMonolithDriver) GetCapabilities() *service.DriverCapabilities {
	return &service.DriverCapabilities{
		Type: DriverType,
		ObjectStorage: service.ObjectStorageCapabilities{
			Operations: []service.ObjectStorageOperation{
				service.ObjectStorageOperationCreate,
				service.ObjectStorageOperationRead,
				service.ObjectStorageOperationWrite,
				service.ObjectStorageOperationDelete,
				service.ObjectStorageOperationList,
				service.ObjectStorageOperationStream,
			},
			AccessModes: []service.ObjectStorageAccessMode{
				service.ObjectStorageAccessReadWrite,
			},
			Constraints: service.ObjectStorageConstraints{
				// Consul KV has a default limit of 512KB per value
				// We set it slightly lower to account for metadata overhead
				MaxObjectSize: 500 * 1024, // 500 KB
				MaxPathLength: 255,
				CaseSensitive: true,
			},
		},
	}
}

// OpenDriver
func (*ConsulMonolithDriver) OpenDriver(ctx context.Context) error {
	return nil
}

// CloseDriver
func (*ConsulMonolithDriver) CloseDriver(ctx context.Context) error {
	return nil
}

// GetObjectStorageService
func (d *ConsulMonolithDriver) GetObjectStorageService() service.ObjectStorageService {
	return &ConsulObjectStorageService{
		driver: d,
	}
}

// GetMetadataService
func (*ConsulMonolithDriver) GetMetadataService() service.MetadataService {
	return nil
}

// GetExtensionService
func (*ConsulMonolithDriver) GetExtensionService(ext service.ServiceExtension) service.Service {
	return nil
}

// parseConsulConfig extracts ConsulBackendConfig from ServiceURI
// URI format: consul[s]://host:port/prefix?token=xxx&datacenter=dc1&namespace=prod
func parseConsulConfig(uri *service.Uri, ssl bool) *ConsulBackendConfig {
	cfg := &ConsulBackendConfig{
		Address:    "127.0.0.1:8500",
		UseSSL:     false,
		Prefix:     "/",
		Token:      "",
		Datacenter: "",
		Namespace:  "",
	}

	// Parse ssl by separating 'consul' and 'consul(s)'
	scheme := "http"
	if ssl {
		scheme = "https"
	}
	// Parse address (host:port) with http/https scheme
	if uri.Host != "" {
		if uri.Port > 0 {
			cfg.Address = fmt.Sprintf("%s://%s:%d", scheme, uri.Host, uri.Port)
		} else {
			cfg.Address = fmt.Sprintf("%s://%s", scheme, uri.Host)
		}
	} else {
		// Default address with scheme
		cfg.Address = fmt.Sprintf("%s://127.0.0.1:8500", scheme)
	}

	// Parse prefix from path
	if uri.Path != "" {
		cfg.Prefix = uri.Path
	}

	// Parse token (from username or query parameter)
	if uri.Username != "" {
		cfg.Token = uri.Username
	} else if token, ok := uri.Query["token"]; ok {
		cfg.Token = token
	}

	// Parse datacenter from query
	if datacenter, ok := uri.Query["datacenter"]; ok {
		cfg.Datacenter = datacenter
	}

	// Parse namespace from query (Consul Enterprise)
	if namespace, ok := uri.Query["namespace"]; ok {
		cfg.Namespace = namespace
	}

	return cfg
}
