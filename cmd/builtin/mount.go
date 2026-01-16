package builtin

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/data/errors"
	"github.com/mwantia/vfs/mount"
	"github.com/mwantia/vfs/mount/builder"
)

func newMountCommand() *cmd.Command {
	c := &cmd.Command{
		Use:   "mount <operation> [args...]",
		Short: "Mount filesystem management",
		Long: `Mount filesystem management with sub-operations:
    attach  - Mount a filesystem at the specified path
    detach  - Unmount a filesystem at the specified path
    update  - Update an existing mount's configuration
    info    - Get information about a specific mount
    list    - List all mounted filesystems`,
		Args: cmd.MinimumArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			operation := strings.TrimSpace(args[0])
			subArgs := args[1:]

			switch operation {
			case "attach":
				return mountOperationAttach(vfs, c, subArgs)
			case "detach":
				return mountOperationDetach(vfs, c, subArgs)
			case "update":
				return mountOperationUpdate(vfs, c, subArgs)
			case "info":
				return mountOperationInfo(vfs, c, subArgs)
			case "list":
				return mountOperationList(vfs, c, subArgs)
			default:
				return fmt.Errorf("unknown operation: %s", operation)
			}
		},
	}

	// Flags for attach operation
	c.Flags().String("namespace", "n", "", "Namespace for the mount")
	c.Flags().String("path-prefix", "p", "", "Path prefix for the mount")
	c.Flags().Bool("readonly", "r", false, "Mount the filesystem read-only")
	c.Flags().Bool("cascading", "c", false, "Enable cascading for child mounts")
	c.Flags().String("metadata", "m", "", "Metadata service URI")
	// Flags for detach operation
	c.Flags().Bool("force", "f", false, "Force unmount even if busy")
	// Common flags
	c.Flags().Bool("verbose", "v", false, "Enable detailed output")
	c.Flags().Bool("json", "j", false, "Output in JSON format")

	return c
}

func mountOperationAttach(vfs cmd.API, cmd *cmd.Command, args []string) error {
	ctx := vfs.GetContext()
	exec := vfs.GetExecutionContext()

	if len(args) < 2 {
		return exec.PrintError("usage: mount attach <path> <service-uri>")
	}

	path := strings.TrimSpace(args[0])
	uri := strings.TrimSpace(args[1])

	// Build mount steps from URI
	steps, err := mount.IdentifyMountSteps(ctx.GetContext(), uri)
	if err != nil {
		return exec.PrintError("failed to identify mount steps: %v", err)
	}

	// Add optional metadata
	if metadata, ok := cmd.Flags().GetString("metadata"); ok && metadata != "" {
		steps = append(steps, builder.WithMetadata(metadata))
	}
	// Add optional namespace
	if namespace, ok := cmd.Flags().GetString("namespace"); ok && namespace != "" {
		steps = append(steps, builder.WithNamespace(namespace))
	}
	// Add optional path prefix
	if pathPrefix, ok := cmd.Flags().GetString("path-prefix"); ok && pathPrefix != "" {
		steps = append(steps, builder.WithPathPrefix(pathPrefix))
	}
	// Add readonly flag
	if readonly, ok := cmd.Flags().GetBool("readonly"); ok && readonly {
		steps = append(steps, builder.AsReadOnly())
	}
	// Add cascading flag
	if cascading, ok := cmd.Flags().GetBool("cascading"); ok && cascading {
		steps = append(steps, builder.AsCascading())
	}
	// Mount the filesystem
	if err := vfs.Mount(ctx.GetContext(), path, steps...); err != nil {
		return exec.PrintError("mount attach: failed to mount %s at %s: %v\n", uri, path, err)
	}

	if v, _ := cmd.Flags().GetBool("verbose"); v {
		return exec.PrintOutput("Mounted %s at %s\n", uri, path)
	}

	return nil
}

func mountOperationDetach(vfs cmd.API, cmd *cmd.Command, args []string) error {
	ctx := vfs.GetContext()
	exec := vfs.GetExecutionContext()

	if len(args) < 1 {
		return exec.PrintError("usage: mount detach [path]... ")
	}

	force, _ := cmd.Flags().GetBool("force")

	errs := errors.Errors{}

	for _, arg := range args {
		path := strings.TrimSpace(arg)
		if err := vfs.Unmount(ctx.GetContext(), path, force); err != nil {
			errs.Add(exec.PrintError("mount detach: failed to unmount %s: %v\n", path, err))
		} else {
			if v, _ := cmd.Flags().GetBool("verbose"); v {
				exec.PrintOutput("Unmounted %s\n", path)
			}
		}
	}

	return errs.Errors()
}

func mountOperationUpdate(vfs cmd.API, cmd *cmd.Command, args []string) error {
	ctx := vfs.GetContext()
	exec := vfs.GetExecutionContext()

	if len(args) < 1 {
		return exec.PrintError("usage: mount update <path>")
	}

	path := strings.TrimSpace(args[0])
	// Build update mask and spec from flags
	var mask builder.MountSpecUpdateMask
	spec := builder.MountSpecifications{
		Options: &builder.MountOptions{},
	}
	// Check for metadata update
	if metadata, ok := cmd.Flags().GetString("metadata"); ok && metadata != "" {
		mask |= builder.MountSpecUpdateMetadata
		spec.Metadata = metadata
	}
	// Check for namespace update
	if namespace, ok := cmd.Flags().GetString("namespace"); ok && namespace != "" {
		mask |= builder.MountSpecUpdateNamespace
		spec.Options.Namespace = namespace
	}
	// Check for path-prefix update
	if pathPrefix, ok := cmd.Flags().GetString("path-prefix"); ok && pathPrefix != "" {
		mask |= builder.MountSpecUpdatePathPrefix
		spec.Options.PathPrefix = pathPrefix
	}
	// Check for readonly update
	if readonly, ok := cmd.Flags().GetBool("readonly"); ok {
		mask |= builder.MountSpecUpdateReadOnly
		spec.Options.IsReadOnly = readonly
	}
	// Check for cascading update
	if cascading, ok := cmd.Flags().GetBool("cascading"); ok {
		mask |= builder.MountSpecUpdateCascading
		spec.Options.Cascading = cascading
	}

	if mask == 0 {
		return exec.PrintError("no update flags specified")
	}

	update := builder.MountSpecUpdate{
		Mask: mask,
		Spec: spec,
	}

	updatedSpec, err := vfs.UpdateMountSpec(ctx.GetContext(), path, update)
	if err != nil {
		return exec.PrintError("mount update: failed to update %s: %v\n", path, err)
	}

	if j, _ := cmd.Flags().GetBool("json"); j {
		data, _ := json.MarshalIndent(updatedSpec, "", "  ")
		return exec.PrintOutput("%v", string(data))
	}

	if v, _ := cmd.Flags().GetBool("verbose"); v {
		return exec.PrintOutput("Updated mount at %s\n", path)
	}

	return nil
}

// mountInfo handles: mount info <path>
func mountOperationInfo(vfs cmd.API, cmd *cmd.Command, args []string) error {
	ctx := vfs.GetContext()
	exec := vfs.GetExecutionContext()

	if len(args) < 1 {
		return exec.PrintError("usage: mount info <path>")
	}

	path := strings.TrimSpace(args[0])

	spec, err := vfs.GetMountSpec(ctx.GetContext(), path)
	if err != nil {
		return exec.PrintError("mount info: failed to get info for %s: %v\n", path, err)
	}

	if j, _ := cmd.Flags().GetBool("json"); j {
		data, _ := json.MarshalIndent(spec, "", "  ")
		return exec.PrintOutput("%v", string(data))
	}

	exec.PrintOutput("Mount: %s\n", path)
	exec.PrintOutput("  ObjectStorage: %s\n", spec.ObjectStorage)
	if spec.Metadata != "" {
		exec.PrintOutput("  Metadata: %s\n", spec.Metadata)
	}
	if spec.Options != nil {
		if spec.Options.Namespace != "" {
			exec.PrintOutput("  Namespace: %s\n", spec.Options.Namespace)
		}
		if spec.Options.PathPrefix != "" {
			exec.PrintOutput("  PathPrefix: %s\n", spec.Options.PathPrefix)
		}
		if spec.Options.IsReadOnly {
			exec.PrintOutput("  ReadOnly: true\n")
		}
		if spec.Options.Cascading {
			exec.PrintOutput("  Cascading: true\n")
		}
	}
	if len(spec.Extensions) > 0 {
		exec.PrintOutput("  Extensions:\n")
		for ext, uri := range spec.Extensions {
			exec.PrintOutput("    %s: %s\n", ext, uri)
		}
	}

	return nil
}

func mountOperationList(vfs cmd.API, cmd *cmd.Command, _ []string) error {
	ctx := vfs.GetContext()
	exec := vfs.GetExecutionContext()

	specs, err := vfs.ListMountSpecs(ctx.GetContext())
	if err != nil {
		return exec.PrintError("mount list: failed to list mounts: %v\n", err)
	}

	if j, _ := cmd.Flags().GetBool("json"); j {
		data, _ := json.MarshalIndent(specs, "", "  ")
		return exec.PrintOutput("%v", string(data))
	}

	if v, _ := cmd.Flags().GetBool("verbose"); v {
		for path, spec := range specs {
			exec.PrintOutput("%s\n", path)
			exec.PrintOutput("  ObjectStorage: %s\n", spec.ObjectStorage)
			if spec.Metadata != "" {
				exec.PrintOutput("  Metadata: %s\n", spec.Metadata)
			}
			if spec.Options != nil && spec.Options.IsReadOnly {
				exec.PrintOutput("  ReadOnly: true\n")
			}
		}
		return nil
	}

	for path, spec := range specs {
		exec.PrintOutput("%s -> %s\n", path, spec.ObjectStorage)
	}

	return nil
}
