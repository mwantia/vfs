package main

import (
	"context"

	"github.com/mwantia/vfs"
	"github.com/mwantia/vfs/mounts"
)

func main() {
	ctx := context.Background()
	fs := vfs.NewVfs()

	if err := fs.Mount(ctx, "/", mounts.NewMemory(), vfs.AsReadOnly(true)); err != nil {
		panic(err)
	}
}
