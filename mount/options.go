package mount

func WithParallel(options *MountStreamerParallelOptions) MountStreamerOption {
	return func(mso *MountStreamerOptions) error {
		mso.Parallel = options
		return nil
	}
}

func WithCoalesce(options *MountStreamerCoalesceOptions) MountStreamerOption {
	return func(mso *MountStreamerOptions) error {
		mso.Coalesce = options
		return nil
	}
}

func WithAdaptive(options *MountStreamerAdaptiveOptions) MountStreamerOption {
	return func(mso *MountStreamerOptions) error {
		mso.Adaptive = options
		return nil
	}
}

func WithConditional(options *MountStreamerConditionalOptions) MountStreamerOption {
	return func(mso *MountStreamerOptions) error {
		mso.Conditional = options
		return nil
	}
}

func WithPrefetch(options *MountStreamerPrefetchOptions) MountStreamerOption {
	return func(mso *MountStreamerOptions) error {
		mso.Prefetch = options
		return nil
	}
}
