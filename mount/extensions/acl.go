package extensions

import "github.com/mwantia/vfs/context"

type AclExtension interface {
	// SetAcl
	SetAcl(traversal context.TraversalContext) error

	// GetAcl
	GetAcl(traversal context.TraversalContext) error
}
