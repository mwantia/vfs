package acl

import (
	"context"

	"github.com/mwantia/vfs/mount/backend"
)

type AclBackendExtension interface {
	backend.Backend

	GetAclPermission(ctx context.Context, path string) (*AclPermission, error)

	SetAclPermission(ctx context.Context, path string, permission *AclPermission) error
}
