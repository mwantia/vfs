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
	return &contextImpl{
		Context: ctx,

		absolutePath:   path,
		relativePath:   path,
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
	return &contextImpl{
		Context: c.Context,

		absolutePath:   c.absolutePath,
		relativePath:   strings.TrimLeft(c.relativePath, path),
		traversalLevel: c.traversalLevel + 1,
	}
}
