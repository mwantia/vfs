package builtin

import (
	"io"
	"strings"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount"
)

func newCopyCommand() *cmd.Command {
	cmd := &cmd.Command{
		Use:   "cp <source> <destination>",
		Short: "Copy files",
		Long:  "Copy a file from source to destination with optional streaming optimizations",
		Args:  cmd.ExactArgsValidator(2),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()

			source := strings.TrimSpace(args[0])
			destination := strings.TrimSpace(args[1])

			opts := make([]mount.MountStreamerOption, 0)
			// Parallel optimization
			if workers, ok := c.Flags().GetInt("parallel-workers"); ok && workers > 0 {
				rangeSizeStr, _ := c.Flags().GetString("parallel-range-size")
				if rangeSizeStr == "" {
					return exec.PrintError("--parallel-workers requires --parallel-range-size")
				}

				rangeSize, err := parseSize(rangeSizeStr)
				if err != nil {
					return exec.PrintError("invalid --parallel-range-size: %v", err)
				}

				opts = append(opts, mount.WithParallel(&mount.MountStreamerParallelOptions{
					Workers:   workers,
					RangeSize: rangeSize,
					Enabled:   true,
				}))
			}
			// Coalesce optimization
			thresholdStr, hasThreshold := c.Flags().GetString("coalesce-threshold")
			sizeStr, hasSize := c.Flags().GetString("coalesce-size")
			if hasThreshold && thresholdStr != "" && hasSize && sizeStr != "" {
				threshold, err := parseSize(thresholdStr)
				if err != nil {
					return exec.PrintError("invalid --coalesce-threshold: %v", err)
				}

				size, err := parseSize(sizeStr)
				if err != nil {
					return exec.PrintError("invalid --coalesce-size: %v", err)
				}

				opts = append(opts, mount.WithCoalesce(&mount.MountStreamerCoalesceOptions{
					Threshold: threshold,
					Size:      size,
					Enabled:   true,
				}))
			} else if hasThreshold != hasSize || (thresholdStr != "" && sizeStr == "") || (thresholdStr == "" && sizeStr != "") {
				return exec.PrintError("--coalesce-threshold and --coalesce-size must both be specified")
			}

			// Adaptive optimization
			minBufferStr, hasMin := c.Flags().GetString("adaptive-min-buffer")
			maxBufferStr, hasMax := c.Flags().GetString("adaptive-max-buffer")
			if hasMin && minBufferStr != "" && hasMax && maxBufferStr != "" {
				minBuffer, err := parseSize(minBufferStr)
				if err != nil {
					return exec.PrintError("invalid --adaptive-min-buffer: %v", err)
				}

				maxBuffer, err := parseSize(maxBufferStr)
				if err != nil {
					return exec.PrintError("invalid --adaptive-max-buffer: %v", err)
				}

				opts = append(opts, mount.WithAdaptive(&mount.MountStreamerAdaptiveOptions{
					MinBuffer:        minBuffer,
					MaxBuffer:        maxBuffer,
					TargetEfficiency: 0.85, // Default target efficiency
					Enabled:          true,
				}))
			} else if hasMin != hasMax || (minBufferStr != "" && maxBufferStr == "") || (minBufferStr == "" && maxBufferStr != "") {
				return exec.PrintError("--adaptive-min-buffer and --adaptive-max-buffer must both be specified")
			}

			// Prefetch optimization
			if prefetchSizeStr, ok := c.Flags().GetString("prefetch-size"); ok && prefetchSizeStr != "" {
				prefetchSize, err := parseSize(prefetchSizeStr)
				if err != nil {
					return exec.PrintError("invalid --prefetch-size: %v", err)
				}

				opts = append(opts, mount.WithPrefetch(&mount.MountStreamerPrefetchOptions{
					Size:    prefetchSize,
					Enabled: true,
				}))
			}

			// Conditional optimization
			if conditional, ok := c.Flags().GetBool("conditional"); ok && conditional {
				opts = append(opts, mount.WithConditional(&mount.MountStreamerConditionalOptions{
					TrackETag: true,
					Enabled:   true,
				}))
			}

			// Open source file for reading with optimizations
			srcStream, err := vfs.OpenFile(ctx.GetContext(), source, data.AccessModeRead, opts...)
			if err != nil {
				return exec.PrintError("cp: %s: %v", source, err)
			}
			defer srcStream.Close()
			// Open destination file for writing
			destStream, err := vfs.OpenFile(ctx.GetContext(), destination, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeTrunc)
			if err != nil {
				return exec.PrintError("cp: %s: %v", source, err)
			}
			defer destStream.Close()
			// Copy data
			if _, err := io.Copy(destStream, srcStream); err != nil {
				return exec.PrintError("cp: error copying %s to %s: %v", source, destination, err)
			}

			return nil
		},
	}

	// Parallel optimization flags
	cmd.Flags().Int("parallel-workers", "", 0, "Number of parallel workers for large reads (0 = disabled)")
	cmd.Flags().String("parallel-range-size", "", "", "Size of each parallel read range (e.g., '1mb', '512kb')")
	// Coalesce optimization flags
	cmd.Flags().String("coalesce-threshold", "", "", "Threshold for small read coalescing (e.g., '64kb')")
	cmd.Flags().String("coalesce-size", "", "", "Buffer size for coalesced reads (e.g., '256kb')")
	// Adaptive optimization flags
	cmd.Flags().String("adaptive-min-buffer", "", "", "Minimum adaptive buffer size (e.g., '256kb')")
	cmd.Flags().String("adaptive-max-buffer", "", "", "Maximum adaptive buffer size (e.g., '10mb')")
	// Prefetch optimization flag
	cmd.Flags().String("prefetch-size", "", "", "Prefetch size for read-ahead (e.g., '5mb')")
	// Conditional optimization flag
	cmd.Flags().Bool("conditional", "", false, "Enable conditional reads with ETag tracking")

	return cmd
}
