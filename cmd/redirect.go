package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/mwantia/vfs/data"
)

// applyRedirects applies all redirects to the execution context
// Returns a cleanup function that should be called after command execution
func (e *Executor) applyRedirects(ctx context.Context, redirects []*Redirect, execCtx *ExecutionContext) (func(), error) {
	var filesToClose []io.Closer
	cleanup := func() {
		for _, f := range filesToClose {
			f.Close()
		}
	}

	for _, redir := range redirects {
		switch redir.Type {
		case RedirectStdinFrom:
			// Open file for reading
			stream, err := e.vfs.OpenFile(ctx, redir.File, data.AccessModeRead)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("failed to open %s for stdin: %w", redir.File, err)
			}
			execCtx.Stdin = stream
			filesToClose = append(filesToClose, stream)

		case RedirectStdoutTo:
			// Open file for writing (truncate)
			stream, err := e.vfs.OpenFile(ctx, redir.File, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeTrunc)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("failed to open %s for stdout: %w", redir.File, err)
			}
			execCtx.Stdout = stream
			filesToClose = append(filesToClose, stream)

		case RedirectStdoutAppend:
			// Open file for appending
			stream, err := e.vfs.OpenFile(ctx, redir.File, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeAppend)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("failed to open %s for stdout (append): %w", redir.File, err)
			}
			execCtx.Stdout = stream
			filesToClose = append(filesToClose, stream)

		case RedirectStderrTo:
			// Open file for stderr
			stream, err := e.vfs.OpenFile(ctx, redir.File, data.AccessModeWrite|data.AccessModeCreate|data.AccessModeTrunc)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("failed to open %s for stderr: %w", redir.File, err)
			}
			execCtx.Stderr = stream
			filesToClose = append(filesToClose, stream)

		case RedirectStderrToStdout:
			// Redirect stderr to stdout (no file needed)
			execCtx.Stderr = execCtx.Stdout

		default:
			cleanup()
			return nil, fmt.Errorf("unsupported redirect type: %v", redir.Type)
		}
	}

	return cleanup, nil
}
