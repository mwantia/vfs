package backend

import (
	"fmt"
	"strings"

	"github.com/mwantia/vfs/pkg/errors"
)

func ParseBackendAddress(address string) (Backend, error) {
	// Format address
	address = strings.TrimSpace(address)
	// Quick check to identify if we work with a possibly valid address
	if !strings.Contains(address, ":") {
		return nil, fmt.Errorf("failed to parse address '%s: %w", address, errors.ErrMalformedBackendAddress)
	}
	// Special 'direct no address declarations'
	switch address {
	case ":ephemeral:":
		return parseEphemeralAddress(), nil
	}
	// Protocol-based parsing
	switch {
	// consul://<address>:<port>?<token>&<readonly>
	case strings.HasPrefix(address, "consul://"):
		return parseConsulAddress(strings.TrimPrefix(address, "consul://"))
		// postgres://<address>:<port>?<ssl>&<readonly>
	case strings.HasPrefix(address, "postgres://"):
		return parsePostgresAddress(strings.TrimPrefix(address, "postgres://"))
	case strings.HasPrefix(address, "postgresql://"):
		return parsePostgresAddress(strings.TrimPrefix(address, "postgresql://"))
	case strings.HasPrefix(address, "psql://"):
		return parsePostgresAddress(strings.TrimPrefix(address, "psql://"))
		// sqlite://<address>:<port>?<readonly>
	case strings.HasPrefix(address, "sqlite://"):
		return parseSqliteAddress(strings.TrimPrefix(address, "sqlite://"))
		// s3://<address>:<port>?<access_key>&<secret_Key>&<ssl>&<readonly>
	case strings.HasPrefix(address, "s3://"):
		return parseS3Address(strings.TrimPrefix(address, "s3://"))
	case strings.HasPrefix(address, "minio://"):
		return parseS3Address(strings.TrimPrefix(address, "minio://"))
	case strings.HasPrefix(address, "rustfs://"):
		return parseS3Address(strings.TrimPrefix(address, "rustfs://"))
		// direct://<address>?<uid>&<gid>&<readonly>
	case strings.HasPrefix(address, "direct://"):
		return parseDirectAddress(strings.TrimPrefix(address, "direct://"))
	}

	return nil, fmt.Errorf("failed to parse address '%s': %w", address, errors.ErrUnknownBackendProtocolAddress)
}

func parseEphemeralAddress() Backend {
	return nil
}

func parseConsulAddress(address string) (Backend, error) {
	return nil, nil
}

func parsePostgresAddress(address string) (Backend, error) {
	return nil, nil
}

func parseSqliteAddress(address string) (Backend, error) {
	return nil, nil
}

func parseS3Address(address string) (Backend, error) {
	return nil, nil
}

func parseDirectAddress(address string) (Backend, error) {
	return nil, nil
}
