package extensions

import "context"

type AclExtension interface {
	// SetAcl
	SetAcl(ctx context.Context) error

	// GetAcl
	GetAcl(ctx context.Context) error
}
