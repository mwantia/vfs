package acl

import "github.com/mwantia/vfs/data"

type AclPermission struct {
	Owner   string                `json:"owner"`
	Group   string                `json:"group"`
	Objects []AclPermissionObject `json:"objects"`
}

type AclPermissionObject struct {
	Type        AclPermissionType    `json:"type"`
	Identifier  string               `json:"identifier"`
	Permissions data.VirtualFileMode `json:"permissions"`
}

type AclPermissionType int

const (
	ACLUser AclPermissionType = iota
	ACLGroup
)
