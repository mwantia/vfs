package context

import (
	"context"
	"fmt"
	"strings"
)

type contextImpl struct {
	context.Context

	absolutePath string
	relativePath string

	traversalLevel int
}

func WithAbsolute(ctx context.Context, path string) TraversalContext {
	// Strip leading slash for relative path (storage keys should not have leading slash)
	relativePath := strings.TrimPrefix(path, "/")

	return &contextImpl{
		Context: ctx,

		absolutePath:   path,
		relativePath:   relativePath,
		traversalLevel: 0,
	}
}

func (c *contextImpl) AbsolutePath() string {
	return c.absolutePath
}

func (c *contextImpl) RelativePath() string {
	return c.relativePath
}

func (c *contextImpl) Depth(level int) error {
	if c.traversalLevel >= level {
		return fmt.Errorf("traversal depth limit reach")
	}

	return nil
}

func (c *contextImpl) Traverse(path string) TraversalContext {
	// Strip the mount prefix from the relative path
	// Normalize the prefix (remove leading/trailing slashes)
	prefix := strings.Trim(path, "/")
	newRelative := c.relativePath

	// Remove the prefix and any following slash
	if prefix != "" {
		if after, ok := strings.CutPrefix(newRelative, prefix+"/"); ok {
			newRelative = after
		} else if newRelative == prefix {
			newRelative = ""
		}
	}

	return &contextImpl{
		Context: c.Context,

		absolutePath:   c.absolutePath,
		relativePath:   newRelative,
		traversalLevel: c.traversalLevel + 1,
	}
}
