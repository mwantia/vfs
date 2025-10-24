package acl

import (
	"context"

	"github.com/mwantia/vfs/backend"
)

type VirtualAclBackend interface {
	backend.VirtualBackend

	GetAclPermission(ctx context.Context, path string) (*AclPermission, error)

	SetAclPermission(ctx context.Context, path string, permission *AclPermission) error
}
