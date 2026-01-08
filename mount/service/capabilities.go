package service

import "slices"

type ServiceType string

const (
	ServiceTypeMetadata      ServiceType = "metadata"
	ServiceTypeObjectStorage ServiceType = "object_storage"
	ServiceTypeMonolith      ServiceType = "monolith"
	ServiceTypeExtension     ServiceType = "extension"
)

type ServiceExtension string

const (
	ServiceExtensionMount        ServiceExtension = "mount"
	ServiceExtensionNotification ServiceExtension = "notification"
	ServiceExtensionACL          ServiceExtension = "acl"
	ServiceExtensionCache        ServiceExtension = "cache"
	ServiceExtensionEncrypt      ServiceExtension = "encrypt"
	ServiceExtensionSnapshot     ServiceExtension = "snapshot"
	ServiceExtensionRubbish      ServiceExtension = "rubbish"
)

type ObjectStorageOperation string

const (
	ObjectStorageOperationCreate      ObjectStorageOperation = "create"
	ObjectStorageOperationRead        ObjectStorageOperation = "read"
	ObjectStorageOperationWrite       ObjectStorageOperation = "write"
	ObjectStorageOperationDelete      ObjectStorageOperation = "delete"
	ObjectStorageOperationList        ObjectStorageOperation = "list"
	ObjectStorageOperationStream      ObjectStorageOperation = "stream"
	ObjectStorageOperationTransaction ObjectStorageOperation = "transaction"
)

type ObjectStorageAccessMode string

const (
	ObjectStorageAccessReadOnly   ObjectStorageAccessMode = "readonly"
	ObjectStorageAccessReadWrite  ObjectStorageAccessMode = "readwrite"
	ObjectStorageAccessWriteOnce  ObjectStorageAccessMode = "writeonce"
	ObjectStorageAccessAppendOnly ObjectStorageAccessMode = "appendonly"
)

type ObjectStorageConstraints struct {
	MinObjectSize    int64 `json:"min_object_size"`
	MaxObjectSize    int64 `json:"max_object_size"`
	MaxPathDepth     int   `json:"max_path_depth"`
	MaxPathLength    int   `json:"max_path_length"`
	CaseSensitive    bool  `json:"case_sensitive"`
	IsReadonly       bool  `json:"is_readonly"`
	SupportSymlinks  bool  `json:"support_symlinks"`
	SupportHardlinks bool  `json:"support_hardlinks"`
}

type ObjectStorageCapabilities struct {
	Operations  []ObjectStorageOperation  `json:"operations"`
	AccessModes []ObjectStorageAccessMode `json:"access_modes"`
	Constraints ObjectStorageConstraints  `json:"constraints"`
}

type DriverCapabilities struct {
	Type          ServiceType               `json:"type"`
	Extensions    []ServiceExtension        `json:"extensions"`
	ObjectStorage ObjectStorageCapabilities `json:"object_storage"`
}

func (sc *DriverCapabilities) IsService(t ServiceType) bool {
	return sc.Type == t
}

func (sc *DriverCapabilities) HasExtension(ext ServiceExtension) bool {
	return slices.Contains(sc.Extensions, ext)
}

func (sc *DriverCapabilities) SupportsOperation(op ObjectStorageOperation) bool {
	return slices.Contains(sc.ObjectStorage.Operations, op)
}

func (sc *DriverCapabilities) AllowsAccess(mode ObjectStorageAccessMode) bool {
	return slices.Contains(sc.ObjectStorage.AccessModes, mode)
}
