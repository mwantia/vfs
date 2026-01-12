package mount

import (
	"context"
	"sync"
	"time"

	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

// Mount
type Mount struct {
	mu         sync.RWMutex
	fstab      map[string]*Mount
	uris       []string
	createTime time.Time

	MountPoint string
	Options    *MountOptions

	ObjectStorage service.ObjectStorageService
	Metadata      service.MetadataService
	Extensions    map[service.ServiceExtension]service.Service
}

// MountOptions
type MountOptions struct {
	Namespace  string
	PathPrefix string
	Cascading  bool
	IsReadOnly bool
}

type MountStreamer struct {
	BaseStreamer

	mu  sync.RWMutex
	ctx context.Context
	mnt MountPoint

	Options *MountStreamerOptions
	Closed  bool

	// Optimization state
	buffer         []byte             // Internal read buffer for coalescing/adaptive
	bufferStart    int64              // Offset of buffer start in file
	bufferEnd      int64              // Offset of buffer end in file
	metrics        []perfMetric       // For adaptive sizing
	etag           string             // For conditional reads
	prefetchData   []byte             // Prefetched data
	prefetchOffset int64              // Offset of prefetched data
	prefetchCancel context.CancelFunc // Cancel prefetch goroutine
}

// perfMetric tracks performance measurements for adaptive buffer sizing
type perfMetric struct {
	bufferSize int64
	totalTime  time.Duration
	bytesRead  int64
	timestamp  time.Time
}

type MountStreamerOptions struct {
	Path       string
	Offset     int64
	Flags      data.AccessMode
	BufferSize int64

	Parallel    *MountStreamerParallelOptions
	Coalesce    *MountStreamerCoalesceOptions
	Adaptive    *MountStreamerAdaptiveOptions
	Conditional *MountStreamerConditionalOptions
	Prefetch    *MountStreamerPrefetchOptions
}

type MountStreamerOption func(*MountStreamerOptions) error

type MountStreamerParallelOptions struct {
	Workers   int   // e.g., 4
	RangeSize int64 // e.g., 1MB
	Enabled   bool
}

type MountStreamerCoalesceOptions struct {
	Threshold int64 // e.g., 64KB - reads smaller than this get coalesced
	Size      int64 // e.g., 256KB - coalesce into this size
	Enabled   bool
}

type MountStreamerAdaptiveOptions struct {
	MinBuffer        int64   // e.g., 256KB
	MaxBuffer        int64   // e.g., 10MB
	TargetEfficiency float64 // e.g., 0.85
	Enabled          bool
}

type MountStreamerConditionalOptions struct {
	TrackETag bool // Use If-Range to detect external changes
	Enabled   bool
}

type MountStreamerPrefetchOptions struct {
	Size    int64 // e.g., 5MB - prefetch this much ahead
	Enabled bool
}
