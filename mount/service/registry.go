package service

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

type Uri struct {
	Scheme   string
	Host     string
	Port     int
	Path     string
	Username string
	Password string
	Query    map[string]string
}

type DriverFactory func(uri *Uri) (Driver, error)

type DriverReference struct {
	Driver Driver
	Count  int
	Opened bool
}

type controllerRegistry struct {
	mu        sync.RWMutex
	factories map[string]DriverFactory
	refs      map[string]*DriverReference
}

var globalRegistry = &controllerRegistry{
	factories: make(map[string]DriverFactory),
	refs:      make(map[string]*DriverReference),
}

func RegisterDriver(scheme string, factory DriverFactory) error {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()

	_, exists := globalRegistry.factories[scheme]
	if exists {
		return fmt.Errorf("schema already registered")
	}

	globalRegistry.factories[scheme] = factory
	return nil
}

// AcquireDriver retrieves or creates a driver for the given URI and increments its reference count.
// If this is the first reference, it calls OpenDriver(ctx) to initialize the driver.
// Each call to AcquireDriver must be matched with a call to ReleaseDriver.
func AcquireDriver(ctx context.Context, uri string) (Driver, error) {
	nuri := normalizeServiceUri(uri)
	if nuri == "" {
		return nil, fmt.Errorf("failed to parse uri")
	}

	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()

	// Check if driver reference already exists
	ref, exists := globalRegistry.refs[nuri]
	if !exists {
		// Parse URI
		parsed, err := parseServiceUri(nuri)
		if err != nil {
			return nil, err
		}

		// Get factory
		factory, exists := globalRegistry.factories[parsed.Scheme]
		if !exists {
			return nil, fmt.Errorf("unknown service type of scheme defined")
		}

		// Create driver instance
		driver, err := factory(parsed)
		if err != nil {
			return nil, fmt.Errorf("failed to create driver: %w", err)
		}

		// Create new reference
		ref = &DriverReference{
			Driver: driver,
		}
		globalRegistry.refs[nuri] = ref
	}

	// Open driver on first reference
	if ref.Count == 0 && !ref.Opened {
		if err := ref.Driver.OpenDriver(ctx); err != nil {
			// Cleanup on open failure
			delete(globalRegistry.refs, nuri)
			return nil, fmt.Errorf("failed to open driver: %w", err)
		}
		ref.Opened = true
	}

	// Increment reference count
	ref.Count++
	return ref.Driver, nil
}

// ReleaseDriver decrements the reference count for the driver at the given URI.
// If the reference count reaches zero, it calls CloseDriver(ctx) and removes the driver from the registry.
func ReleaseDriver(ctx context.Context, uri string) error {
	nuri := normalizeServiceUri(uri)
	if nuri == "" {
		return fmt.Errorf("failed to parse uri")
	}

	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()

	ref, exists := globalRegistry.refs[nuri]
	if !exists {
		return fmt.Errorf("driver not registered for uri: %s", nuri)
	}

	// Decrement reference count
	ref.Count--

	// Close driver when no more references
	if ref.Count <= 0 {
		if ref.Opened {
			if err := ref.Driver.CloseDriver(ctx); err != nil {
				return fmt.Errorf("failed to close driver: %w", err)
			}
			ref.Opened = false
		}
		delete(globalRegistry.refs, nuri)
	}

	return nil
}

func normalizeServiceUri(uri string) string {
	return strings.TrimSpace(uri)
}

func parseServiceUri(uri string) (*Uri, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse - invalid uri")
	}

	parsed := &Uri{
		Scheme: u.Scheme,
		Host:   u.Hostname(),
		Path:   strings.TrimPrefix(u.Path, "/"),
		Query:  make(map[string]string),
	}

	if u.Port() != "" {
		parsed.Port, err = strconv.Atoi(u.Port())
		if err != nil {
			return nil, fmt.Errorf("failed to parse - invalid port defined")
		}
	}

	if u.User != nil {
		parsed.Username = u.User.Username()
		parsed.Password, _ = u.User.Password()
	}

	for key, value := range u.Query() {
		if len(value) > 0 {
			parsed.Query[key] = value[0]
		}
	}

	return parsed, nil
}
