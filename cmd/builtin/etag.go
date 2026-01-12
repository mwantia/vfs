package builtin

import (
	"crypto/md5"
	"encoding/hex"
	"io"

	"github.com/mwantia/vfs/cmd"
	"github.com/mwantia/vfs/data"
)

func newETagCommand() *cmd.Command {
	c := &cmd.Command{
		Use:   "etag <path>",
		Short: "Display or generate ETag hash for a file",
		Long:  "Display the ETag hash for a file. Returns stored ETag from metadata or calculates S3-style multipart ETag from file content.",
		Args:  cmd.ExactArgsValidator(1),
		Run: func(vfs cmd.API, c *cmd.Command, args []string) error {
			ctx := vfs.GetContext()
			exec := vfs.GetExecutionContext()
			path := args[0]

			// Parse chunk size
			size := int64(8 * 1024 * 1024) // Default 8MB
			if chunkSize, _ := c.Flags().GetString("chunk-size"); chunkSize != "" {
				parsedSize, err := parseSize(chunkSize)
				if err != nil {
					return exec.PrintError("etag: invalid chunk-size: %v\n", err)
				}
				size = parsedSize
			}

			// First, try to get metadata with ETag
			stat, err := vfs.StatMetadata(ctx.GetContext(), path)
			if err != nil {
				return exec.PrintError("etag: %s: %v\n", path, err)
			}

			// Check if it's a directory
			if stat.Mode.IsDir() {
				return exec.PrintError("etag: %s: is a directory\n", path)
			}

			// If ETag exists in metadata and we're not skipping, use it
			if skip, _ := c.Flags().GetBool("skip"); skip && stat.ETag != "" {
				return exec.PrintOutput("%s\n", stat.ETag)
			}

			// Calculate ETag from file content
			stream, err := vfs.OpenFile(ctx.GetContext(), path, data.AccessModeRead)
			if err != nil {
				return err
			}
			defer stream.Close()

			var md5Hashes [][]byte
			buffer := make([]byte, size)

			for {
				n, err := io.ReadFull(stream, buffer)
				if n > 0 {
					// Calculate MD5 for this chunk
					hasher := md5.New()
					hasher.Write(buffer[:n])
					md5Hashes = append(md5Hashes, hasher.Sum(nil))
				}

				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				if err != nil {
					return err
				}
			}

			// Empty file
			if len(md5Hashes) < 1 {
				hash := hex.EncodeToString(md5.New().Sum(nil))
				return exec.PrintOutput("%s\n", hash)
			}

			// Single chunk - return its MD5
			if len(md5Hashes) == 1 {
				hash := hex.EncodeToString(md5Hashes[0])
				return exec.PrintOutput("%s\n", hash)
			}

			// Multiple chunks - concatenate digests, MD5 that, append part count
			var digests []byte
			for _, hash := range md5Hashes {
				digests = append(digests, hash...)
			}

			finalHasher := md5.New()
			finalHasher.Write(digests)

			return exec.PrintOutput("%s-%d\n", hex.EncodeToString(finalHasher.Sum(nil)), len(md5Hashes))
		},
	}

	c.Flags().Bool("skip", "s", false, "Skip metadata and always generate ETag from file content")
	c.Flags().String("chunk-size", "", "8mb", "Chunk size for multipart ETag calculation (e.g., '8mb', '5mb', '512kb')")
	return c
}
