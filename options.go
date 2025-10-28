package vfs

import "github.com/mwantia/vfs/log"

type VirtualFileSystemOptions struct {
	LogLevel      log.LogLevel
	LogFile       string
	NoTerminalLog bool
	BaseRootMount bool
}

type VirtualFileSystemOption func(*VirtualFileSystemOptions) error

func newDefaultVirtualFileSystemOptions() *VirtualFileSystemOptions {
	return &VirtualFileSystemOptions{
		LogLevel: log.Info,
	}
}

func WithLogLevel(logLevel log.LogLevel) VirtualFileSystemOption {
	return func(opts *VirtualFileSystemOptions) error {
		opts.LogLevel = logLevel
		return nil
	}
}

func WithoutTerminalLog() VirtualFileSystemOption {
	return func(opts *VirtualFileSystemOptions) error {
		opts.NoTerminalLog = true
		return nil
	}
}

func WithLogFile(logFile string) VirtualFileSystemOption {
	return func(opts *VirtualFileSystemOptions) error {
		opts.LogFile = logFile
		return nil
	}
}

func WithBaseRootMount() VirtualFileSystemOption {
	return func(opts *VirtualFileSystemOptions) error {
		opts.BaseRootMount = true
		return nil
	}
}
