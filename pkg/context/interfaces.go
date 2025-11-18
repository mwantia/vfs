package context

import "context"

type TraversalContext interface {
	context.Context

	AbsolutePath() string
	RelativePath() string

	Depth(level int) error
	Traverse(path string) TraversalContext
}
