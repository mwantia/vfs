package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mwantia/vfs"
	_ "github.com/mwantia/vfs/mount/service/ephemeral" // Import to register driver
)

func main() {
	// Create a new VFS instance
	fs, err := vfs.NewVirtualFileSystem()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create VFS: %v\n", err)
		os.Exit(1)
	}
	defer fs.Shutdown(context.Background())

	ctx := context.Background()

	// Example 1: Mount a filesystem
	fmt.Println("=== Example 1: Mount a filesystem ===")
	code, err := fs.Execute(ctx, os.Stdout, "mount", "ephemeral://", "/")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	// Example 2: Create a directory
	fmt.Println("=== Example 2: Create a directory ===")
	code, err = fs.Execute(ctx, os.Stdout, "mkdir", "/test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	// Example 3: Write a file using echo and redirection
	fmt.Println("=== Example 3: Write a file using echo and redirection ===")
	code, err = fs.Execute(ctx, os.Stdout, "echo \"Hello, VFS!\" > /test/hello.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	// Example 4: Read the file with cat
	fmt.Println("=== Example 4: Read the file with cat ===")
	code, err = fs.Execute(ctx, os.Stdout, "cat", "/test/hello.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	// Example 5: List directory contents
	fmt.Println("=== Example 5: List directory contents ===")
	code, err = fs.Execute(ctx, os.Stdout, "ls", "-l", "/test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	// Example 6: Use a pipeline (cat | tee)
	fmt.Println("=== Example 6: Use a pipeline (cat | tee) ===")
	code, err = fs.Execute(ctx, os.Stdout, "cat /test/hello.txt | tee /test/copy.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	// Example 7: Verify the copy was created
	fmt.Println("=== Example 7: Verify the copy was created ===")
	code, err = fs.Execute(ctx, os.Stdout, "stat", "/test/copy.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	// Example 8: Show help
	fmt.Println("=== Example 8: Show help ===")
	code, err = fs.Execute(ctx, os.Stdout, "help")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	// Example 9: Show detailed help for a command
	fmt.Println("=== Example 9: Show detailed help for cat ===")
	code, err = fs.Execute(ctx, os.Stdout, "help", "cat")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error (code %d): %v\n", code, err)
	}
	fmt.Println()

	fmt.Println("=== All examples completed successfully! ===")
}
