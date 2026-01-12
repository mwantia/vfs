package mount

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/mwantia/vfs/data"
)

func NewStreamer(ctx context.Context, mnt MountPoint, options *MountStreamerOptions) Streamer {
	if options.BufferSize <= 0 {
		// Default buffer size of 1 MB
		options.BufferSize = 1 * 1024 * 1024
	}

	ms := &MountStreamer{
		ctx: ctx,
		mnt: mnt,

		Options: options,
		Closed:  false,

		// Initialize optimization state
		buffer:      nil,
		bufferStart: 0,
		bufferEnd:   0,
		metrics:     make([]perfMetric, 0, 10),
	}

	// Initialize conditional tracking if enabled
	if options.Conditional != nil && options.Conditional.Enabled {
		ms.initConditional()
	}

	// Start prefetch goroutine if enabled
	if options.Prefetch != nil && options.Prefetch.Enabled {
		ms.startPrefetch()
	}

	return ms
}

func (ms *MountStreamer) IsBusy() bool {
	// Try to acquire the lock - if we can't immediately, the file is busy
	if !ms.mu.TryLock() {
		return true
	}
	// We got the lock, so it's not busy - release it
	ms.mu.Unlock()

	return false
}

// CanRead returns true if the virtual file can be read, otherwise false.
func (ms *MountStreamer) CanRead() bool {
	return ms.Options.Flags&data.AccessModeRead != 0
}

// CanWrite returns true if the virtual file can be written, otherwise false.
func (ms *MountStreamer) CanWrite() bool {
	return ms.Options.Flags&data.AccessModeWrite != 0
}

// Read reads up to len(p) bytes from the file at the current offset.
// Advances the offset by the number of bytes read.
// Returns ErrPermission if the file was not opened with read access.
func (ms *MountStreamer) Read(p []byte) (n int, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.Closed {
		return 0, data.ErrClosed
	}
	// Check if file was opened with read access
	if !ms.CanRead() {
		return 0, data.ErrPermission
	}

	// Route to appropriate optimization based on enabled options
	// Priority order: Parallel > Coalesce > Adaptive > Direct
	if ms.Options.Parallel != nil && ms.Options.Parallel.Enabled {
		return ms.readParallel(p)
	}
	if ms.Options.Coalesce != nil && ms.Options.Coalesce.Enabled {
		return ms.readCoalesced(p)
	}
	if ms.Options.Adaptive != nil && ms.Options.Adaptive.Enabled {
		return ms.readAdaptive(p)
	}

	// Fallback to direct read
	return ms.readDirect(p)
}

// readDirect performs a direct read without any optimizations.
// NOTE: Mutex already held by caller.
func (ms *MountStreamer) readDirect(p []byte) (n int, err error) {
	// Read from mount
	buffer, err := ms.mnt.ReadFile(ms.ctx, ms.Options.Path, ms.Options.Offset, int64(len(p)))
	if err != nil {
		return 0, err
	}
	// Copy to destination buffer
	n = copy(p, buffer)
	// Update offset
	ms.Options.Offset += int64(n)
	// Return EOF if no bytes read
	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

// readCoalesced buffers large reads to serve small requests efficiently.
// For reads smaller than Threshold, it fetches larger chunks (Size) and serves from buffer.
// NOTE: Mutex already held by caller.
func (ms *MountStreamer) readCoalesced(p []byte) (n int, err error) {
	cfg := ms.Options.Coalesce

	// If read is large, bypass coalescing
	if int64(len(p)) >= cfg.Threshold {
		return ms.readDirect(p)
	}

	// Check if buffer has data at current offset
	if ms.bufferStart <= ms.Options.Offset && ms.Options.Offset < ms.bufferEnd {
		// Serve from buffer
		bufferOffset := ms.Options.Offset - ms.bufferStart
		n = copy(p, ms.buffer[bufferOffset:])
		ms.Options.Offset += int64(n)

		if n == 0 {
			return 0, io.EOF
		}
		return n, nil
	}

	// Buffer miss - refill with larger read
	coalesceSize := cfg.Size
	readData, err := ms.mnt.ReadFile(ms.ctx, ms.Options.Path, ms.Options.Offset, coalesceSize)
	if err != nil && err != io.EOF {
		return 0, err
	}

	// Update buffer state
	ms.buffer = make([]byte, len(readData))
	copy(ms.buffer, readData)
	ms.bufferStart = ms.Options.Offset
	ms.bufferEnd = ms.Options.Offset + int64(len(readData))

	// Serve from newly filled buffer
	n = copy(p, readData)
	ms.Options.Offset += int64(n)

	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// readAdaptive uses dynamic buffer sizing based on performance metrics.
// Adjusts buffer size to optimize for throughput efficiency.
// NOTE: Mutex already held by caller.
func (ms *MountStreamer) readAdaptive(p []byte) (n int, err error) {
	cfg := ms.Options.Adaptive

	// Initialize buffer size if not set
	if ms.buffer == nil || (ms.bufferStart == 0 && ms.bufferEnd == 0) {
		ms.buffer = make([]byte, cfg.MinBuffer)
	}

	// Check if buffer has data at current offset
	if ms.bufferStart <= ms.Options.Offset && ms.Options.Offset < ms.bufferEnd {
		// Serve from buffer
		bufferOffset := ms.Options.Offset - ms.bufferStart
		n = copy(p, ms.buffer[bufferOffset:])
		ms.Options.Offset += int64(n)

		if n == 0 {
			return 0, io.EOF
		}
		return n, nil
	}

	// Refill buffer with adaptive size
	start := time.Now()
	currentBufferSize := int64(len(ms.buffer))

	readData, err := ms.mnt.ReadFile(ms.ctx, ms.Options.Path, ms.Options.Offset, currentBufferSize)
	totalTime := time.Since(start)

	if err != nil && err != io.EOF {
		return 0, err
	}

	// Record measurement
	ms.recordMetric(perfMetric{
		bufferSize: currentBufferSize,
		totalTime:  totalTime,
		bytesRead:  int64(len(readData)),
		timestamp:  time.Now(),
	})

	// Adjust buffer size based on metrics
	ms.adjustBufferSize(cfg)

	// Update buffer state
	ms.buffer = make([]byte, len(readData))
	copy(ms.buffer, readData)
	ms.bufferStart = ms.Options.Offset
	ms.bufferEnd = ms.Options.Offset + int64(len(readData))

	// Serve from buffer
	n = copy(p, readData)
	ms.Options.Offset += int64(n)

	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// recordMetric adds a performance measurement to the metrics history.
// Keeps only the last 10 measurements.
func (ms *MountStreamer) recordMetric(m perfMetric) {
	ms.metrics = append(ms.metrics, m)
	// Keep last 10 measurements only
	if len(ms.metrics) > 10 {
		ms.metrics = ms.metrics[1:]
	}
}

// adjustBufferSize dynamically adjusts the buffer size based on throughput trends.
func (ms *MountStreamer) adjustBufferSize(cfg *MountStreamerAdaptiveOptions) {
	if len(ms.metrics) < 3 {
		return // Need more data
	}

	// Calculate average efficiency (throughput)
	totalEfficiency := 0.0
	for _, m := range ms.metrics {
		if m.totalTime > 0 {
			// Efficiency = bytes_read / time (throughput)
			efficiency := float64(m.bytesRead) / float64(m.totalTime.Microseconds())
			totalEfficiency += efficiency
		}
	}
	avgEfficiency := totalEfficiency / float64(len(ms.metrics))

	// Get recent trend (last 3 measurements)
	recentEfficiency := 0.0
	for i := len(ms.metrics) - 3; i < len(ms.metrics); i++ {
		m := ms.metrics[i]
		if m.totalTime > 0 {
			recentEfficiency += float64(m.bytesRead) / float64(m.totalTime.Microseconds())
		}
	}
	recentEfficiency /= 3.0

	currentSize := int64(len(ms.buffer))

	// If recent efficiency is declining, we may be over-buffering
	if recentEfficiency < avgEfficiency*0.9 && currentSize > cfg.MinBuffer {
		newSize := int64(float64(currentSize) * 0.85)
		if newSize < cfg.MinBuffer {
			newSize = cfg.MinBuffer
		}
		ms.buffer = make([]byte, newSize)
	} else if recentEfficiency > avgEfficiency*1.1 && currentSize < cfg.MaxBuffer {
		// Efficiency improving, increase buffer
		newSize := int64(float64(currentSize) * 1.2)
		if newSize > cfg.MaxBuffer {
			newSize = cfg.MaxBuffer
		}
		ms.buffer = make([]byte, newSize)
	}
}

// readParallel splits large reads into concurrent range fetches for improved throughput.
// Releases the mutex during concurrent fetches, then reacquires to update state.
// NOTE: Mutex already held by caller.
func (ms *MountStreamer) readParallel(p []byte) (n int, err error) {
	cfg := ms.Options.Parallel

	// Small reads don't benefit from parallelism
	minParallelSize := cfg.RangeSize * int64(cfg.Workers)
	if int64(len(p)) < minParallelSize {
		return ms.readDirect(p)
	}

	// Calculate ranges
	rangeSize := cfg.RangeSize
	numRanges := (int64(len(p)) + rangeSize - 1) / rangeSize
	if numRanges > int64(cfg.Workers) {
		numRanges = int64(cfg.Workers)
	}

	// Save current offset and path (needed after unlock)
	startOffset := ms.Options.Offset
	path := ms.Options.Path
	ctx := ms.ctx

	// CRITICAL: Release lock during concurrent fetches
	ms.mu.Unlock()

	// Fetch ranges concurrently
	type rangeResult struct {
		index int
		data  []byte
		err   error
	}

	results := make(chan rangeResult, numRanges)

	for i := int64(0); i < numRanges; i++ {
		go func(idx int64) {
			offset := startOffset + (idx * rangeSize)
			size := rangeSize

			// Last range may be smaller
			remaining := int64(len(p)) - (idx * rangeSize)
			if size > remaining {
				size = remaining
			}

			// Read this range
			data, err := ms.mnt.ReadFile(ctx, path, offset, size)

			results <- rangeResult{
				index: int(idx),
				data:  data,
				err:   err,
			}
		}(i)
	}

	// Collect results
	rangeData := make([][]byte, numRanges)
	var firstError error

	for i := int64(0); i < numRanges; i++ {
		result := <-results
		if result.err != nil && result.err != io.EOF && firstError == nil {
			firstError = result.err
		}
		rangeData[result.index] = result.data
	}

	// CRITICAL: Reacquire lock before updating state
	ms.mu.Lock()

	// Check if closed while we were unlocked
	if ms.Closed {
		return 0, data.ErrClosed
	}

	// Return error if any occurred
	if firstError != nil {
		return 0, firstError
	}

	// Assemble ranges into output buffer
	offset := 0
	for _, rangeBytes := range rangeData {
		copied := copy(p[offset:], rangeBytes)
		offset += copied
	}

	n = offset
	ms.Options.Offset += int64(n)

	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

// Write writes len(p) bytes to the file at the current offset.
// Advances the offset by the number of bytes written.
// Returns ErrPermission if the file was not opened with write access.
func (ms *MountStreamer) Write(p []byte) (n int, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.Closed {
		return 0, data.ErrClosed
	}
	// Check if file was opened with write access
	if !ms.CanWrite() {
		return 0, data.ErrPermission
	}

	// Invalidate read buffers on write
	ms.bufferStart = 0
	ms.bufferEnd = 0
	ms.prefetchData = nil

	// Write to mount
	n, err = ms.mnt.WriteFile(ms.ctx, ms.Options.Path, ms.Options.Offset, p)
	if err != nil {
		return 0, err
	}
	// Update offset
	ms.Options.Offset += int64(n)

	return n, nil
}

// Seek sets the offset for the next Read or Write operation and returns the new offset.
func (ms *MountStreamer) Seek(offset int64, whence int) (int64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.Closed {
		return 0, data.ErrClosed
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = ms.Options.Offset + offset
	case io.SeekEnd:
		// Need to get file size to seek from end
		meta, err := ms.mnt.StatMetadata(ms.ctx, ms.Options.Path)
		if err != nil {
			return 0, err
		}
		newOffset = meta.Size + offset
	default:
		return 0, data.ErrInvalid
	}
	// Ensure offset is non-negative
	if newOffset < 0 {
		return 0, data.ErrInvalid
	}

	// Invalidate buffers if offset changed
	if newOffset != ms.Options.Offset {
		ms.bufferStart = 0
		ms.bufferEnd = 0
		ms.prefetchData = nil
	}

	ms.Options.Offset = newOffset
	return newOffset, nil
}

// Close marks the file stream as closed and unregisters it from the VFS.
func (ms *MountStreamer) Close() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Fail operations if streamer has been closed
	if ms.Closed {
		return data.ErrClosed
	}

	// Cancel prefetch goroutine if running
	if ms.prefetchCancel != nil {
		ms.prefetchCancel()
	}

	// Close the file in the mount
	if err := ms.mnt.CloseFile(ms.ctx, ms.Options.Path, false); err != nil {
		return err
	}

	// Only mark as closed after successful close
	ms.Closed = true
	return nil
}

// initConditional initializes ETag tracking for conditional reads.
func (ms *MountStreamer) initConditional() error {
	if ms.Options.Conditional == nil || !ms.Options.Conditional.Enabled {
		return nil
	}

	// Get initial ETag from metadata
	meta, err := ms.mnt.StatMetadata(ms.ctx, ms.Options.Path)
	if err != nil {
		return err
	}

	// Use modification time and size as pseudo-ETag (real ETag needs backend support)
	ms.etag = fmt.Sprintf("%d-%d", meta.Size, meta.ModifyTime.Unix())
	return nil
}

// validateConditional checks if the file changed externally and invalidates buffers if needed.
func (ms *MountStreamer) validateConditional() error {
	if ms.Options.Conditional == nil || !ms.Options.Conditional.Enabled {
		return nil
	}

	// Check if file changed externally
	meta, err := ms.mnt.StatMetadata(ms.ctx, ms.Options.Path)
	if err != nil {
		return err
	}

	currentETag := fmt.Sprintf("%d-%d", meta.Size, meta.ModifyTime.Unix())
	if currentETag != ms.etag {
		// File changed externally - invalidate buffer
		ms.bufferStart = 0
		ms.bufferEnd = 0
		ms.etag = currentETag
	}

	return nil
}

// startPrefetch starts a background goroutine that prefetches data ahead of current offset.
func (ms *MountStreamer) startPrefetch() {
	if ms.Options.Prefetch == nil || !ms.Options.Prefetch.Enabled {
		return
	}

	ctx, cancel := context.WithCancel(ms.ctx)
	ms.prefetchCancel = cancel

	go ms.prefetchWorker(ctx)
}

// prefetchWorker runs in background and prefetches data ahead of the current read position.
func (ms *MountStreamer) prefetchWorker(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ms.mu.Lock()
			if ms.Closed {
				ms.mu.Unlock()
				return
			}

			// Prefetch ahead of current offset
			nextOffset := ms.Options.Offset + ms.Options.Prefetch.Size
			path := ms.Options.Path
			size := ms.Options.Prefetch.Size

			ms.mu.Unlock()

			// Fetch without holding lock
			data, err := ms.mnt.ReadFile(ctx, path, nextOffset, size)
			if err != nil {
				continue
			}

			ms.mu.Lock()
			ms.prefetchData = data
			ms.prefetchOffset = nextOffset
			ms.mu.Unlock()
		}
	}
}

func (ms *MountStreamer) ReadFrom(r io.Reader) (int64, error) {
	// Use a custom buffer for bulk reads
	buffer := make([]byte, ms.Options.BufferSize)

	var total int64
	for {
		n, err := r.Read(buffer)
		if n > 0 {
			wrote, writeErr := ms.Write(buffer[:n])
			total += int64(wrote)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}

func (ms *MountStreamer) WriteTo(w io.Writer) (int64, error) {
	// Use a custom buffer for bulk writes
	buffer := make([]byte, ms.Options.BufferSize)

	var total int64
	for {
		n, err := ms.Read(buffer)
		if n > 0 {
			wrote, writeErr := w.Write(buffer[:n])
			total += int64(wrote)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}
