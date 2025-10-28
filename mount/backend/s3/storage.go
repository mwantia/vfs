package s3

import (
	"bytes"
	"context"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/data/errors"
)

func (sb *S3Backend) CreateObject(ctx context.Context, key string, mode data.VirtualFileMode) (*data.VirtualFileStat, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	now := time.Now()

	// Check if object already exists
	_, err := sb.client.StatObject(ctx, sb.bucketName, key, minio.StatObjectOptions{})
	if err == nil {
		return nil, data.ErrExist
	}

	// For directories, create a zero-byte object with trailing slash
	if mode.IsDir() {
		if !strings.HasSuffix(key, "/") {
			key += "/"
		}
		_, err = sb.client.PutObject(ctx, sb.bucketName, key, bytes.NewReader([]byte{}), 0, minio.PutObjectOptions{
			ContentType: "application/x-directory",
		})
		if err != nil {
			return nil, err
		}
	} else {
		// For files, create empty object
		_, err = sb.client.PutObject(ctx, sb.bucketName, key, bytes.NewReader([]byte{}), 0, minio.PutObjectOptions{})
		if err != nil {
			return nil, err
		}
	}

	return &data.VirtualFileStat{
		Key:  key,
		Mode: mode,
		Size: 0,

		CreateTime: now,
		ModifyTime: now,
	}, nil
}

func (sb *S3Backend) ReadObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	// Get object info to check if it exists and is not a directory
	objInfo, err := sb.client.StatObject(ctx, sb.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return 0, data.ErrNotExist
		}
		return 0, err
	}

	// Check if it's a directory (trailing slash or zero-byte with directory content type)
	if strings.HasSuffix(objInfo.Key, "/") || objInfo.ContentType == "application/x-directory" {
		return 0, data.ErrIsDirectory
	}

	// Read object with offset
	opts := minio.GetObjectOptions{}
	if offset > 0 {
		if err := opts.SetRange(offset, offset+int64(len(dat))-1); err != nil {
			return 0, err
		}
	}

	object, err := sb.client.GetObject(ctx, sb.bucketName, key, opts)
	if err != nil {
		return 0, err
	}
	defer object.Close()

	n, err := io.ReadFull(object, dat)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return n, err
	}

	return n, nil
}

func (sb *S3Backend) WriteObject(ctx context.Context, key string, offset int64, dat []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// S3 doesn't support partial writes - we need to read-modify-write
	// First, check if object exists
	objInfo, err := sb.client.StatObject(ctx, sb.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return 0, data.ErrNotExist
		}
		return 0, err
	}

	// Check if it's a directory
	if strings.HasSuffix(objInfo.Key, "/") || objInfo.ContentType == "application/x-directory" {
		return 0, data.ErrIsDirectory
	}

	// Read existing content
	var existingData []byte
	if objInfo.Size > 0 {
		object, err := sb.client.GetObject(ctx, sb.bucketName, key, minio.GetObjectOptions{})
		if err != nil {
			return 0, err
		}
		existingData, err = io.ReadAll(object)
		object.Close()
		if err != nil {
			return 0, err
		}
	}

	// Calculate new size
	writeEnd := offset + int64(len(dat))
	newSize := max(writeEnd, int64(len(existingData)))

	// Create new buffer
	newData := make([]byte, newSize)
	if len(existingData) > 0 {
		copy(newData, existingData)
	}

	// Write new data at offset
	copy(newData[offset:], dat)

	// Upload the modified object
	_, err = sb.client.PutObject(ctx, sb.bucketName, key, bytes.NewReader(newData), int64(len(newData)), minio.PutObjectOptions{
		ContentType: objInfo.ContentType,
	})
	if err != nil {
		return 0, err
	}

	return len(dat), nil
}

func (sb *S3Backend) DeleteObject(ctx context.Context, key string, force bool) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Check if object exists
	objInfo, err := sb.client.StatObject(ctx, sb.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return data.ErrNotExist
		}
		return err
	}

	// Check if it's a directory
	isDir := strings.HasSuffix(objInfo.Key, "/") || objInfo.ContentType == "application/x-directory"

	if isDir {
		if !force {
			return data.ErrIsDirectory
		}

		// Delete directory and all its contents
		prefix := key
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

		// List all objects with this prefix
		objectsCh := sb.client.ListObjects(ctx, sb.bucketName, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		})

		// Collect objects to delete
		var objectsToDelete []minio.ObjectInfo
		objectsToDelete = append(objectsToDelete, objInfo) // Include the directory itself

		for object := range objectsCh {
			if object.Err != nil {
				return object.Err
			}
			objectsToDelete = append(objectsToDelete, object)
		}

		errs := errors.Errors{}
		// Delete all objects
		for _, obj := range objectsToDelete {
			if err := sb.client.RemoveObject(ctx, sb.bucketName, obj.Key, minio.RemoveObjectOptions{}); err != nil {
				errs.Add(err)
			}
		}

		return errs.Errors()
	}

	// Delete single object
	return sb.client.RemoveObject(ctx, sb.bucketName, key, minio.RemoveObjectOptions{})
}

func (sb *S3Backend) ListObjects(ctx context.Context, key string) ([]*data.VirtualFileStat, error) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	// For non-empty keys, check if the key itself is an object
	var err error
	if key != "" {
		objInfo, statErr := sb.client.StatObject(ctx, sb.bucketName, key, minio.StatObjectOptions{})
		if statErr == nil {
			// Object exists
			isDir := strings.HasSuffix(objInfo.Key, "/") || objInfo.ContentType == "application/x-directory"

			if !isDir {
				// It's a file, return just this object
				return []*data.VirtualFileStat{
					sb.toVirtualFileStat(key, objInfo),
				}, nil
			}

			// It's a directory, list its contents
			prefix := key
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}

			// List objects with delimiter to get only direct children
			objectsCh := sb.client.ListObjects(ctx, sb.bucketName, minio.ListObjectsOptions{
				Prefix:    prefix,
				Recursive: false,
			})

			var stats []*data.VirtualFileStat
			for object := range objectsCh {
				if object.Err != nil {
					return nil, object.Err
				}

				// Skip the directory object itself
				if object.Key == prefix {
					continue
				}

				// Get relative key (remove prefix)
				relKey := strings.TrimPrefix(object.Key, prefix)

				// Skip if relative key is empty (shouldn't happen, but be defensive)
				if relKey == "" {
					continue
				}

				stats = append(stats, sb.toVirtualFileStat(relKey, object))
			}

			return stats, nil
		}
		err = statErr
	}

	// Object doesn't exist (or key is empty for root), treat as implicit directory
	if key == "" || (err != nil && minio.ToErrorResponse(err).Code == "NoSuchKey") {
		// Try listing with prefix to see if there are objects under this path
		prefix := key
		if key != "" && !strings.HasSuffix(key, "/") {
			prefix += "/"
		}

		objectsCh := sb.client.ListObjects(ctx, sb.bucketName, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: false,
		})

		var stats []*data.VirtualFileStat
		hasObjects := false

		for object := range objectsCh {
			if object.Err != nil {
				return nil, object.Err
			}

			hasObjects = true
			relKey := strings.TrimPrefix(object.Key, prefix)

			// Skip if relative key is empty (directory object itself)
			if relKey == "" {
				continue
			}

			stats = append(stats, sb.toVirtualFileStat(relKey, object))
		}

		if !hasObjects {
			return nil, data.ErrNotExist
		}

		return stats, nil
	}

	return nil, err
}

func (sb *S3Backend) HeadObject(ctx context.Context, key string) (*data.VirtualFileStat, error) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	// Handle empty key (root of bucket) - return synthetic directory stat
	if key == "" {
		return &data.VirtualFileStat{
			Key:        "",
			Size:       0,
			Mode:       data.ModeDir | 0755,
			ModifyTime: time.Now(),
			CreateTime: time.Now(),
		}, nil
	}

	objInfo, err := sb.client.StatObject(ctx, sb.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, data.ErrNotExist
		}
		return nil, err
	}

	return sb.toVirtualFileStat(key, objInfo), nil
}

func (sb *S3Backend) TruncateObject(ctx context.Context, key string, size int64) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Check if object exists
	objInfo, err := sb.client.StatObject(ctx, sb.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return data.ErrNotExist
		}
		return err
	}

	// Check if it's a directory
	if strings.HasSuffix(objInfo.Key, "/") || objInfo.ContentType == "application/x-directory" {
		return data.ErrIsDirectory
	}

	// If size is the same, no need to do anything
	if objInfo.Size == size {
		return nil
	}

	var newData []byte

	if size > 0 {
		if objInfo.Size > 0 {
			// Read existing content
			object, err := sb.client.GetObject(ctx, sb.bucketName, key, minio.GetObjectOptions{})
			if err != nil {
				return err
			}
			existingData, err := io.ReadAll(object)
			object.Close()
			if err != nil {
				return err
			}

			if size < objInfo.Size {
				// Truncate to smaller size
				newData = existingData[:size]
			} else {
				// Expand with zeros
				newData = make([]byte, size)
				copy(newData, existingData)
			}
		} else {
			// Existing file is empty, create new buffer with zeros
			newData = make([]byte, size)
		}
	}

	// Upload the truncated/expanded object
	_, err = sb.client.PutObject(ctx, sb.bucketName, key, bytes.NewReader(newData), int64(len(newData)), minio.PutObjectOptions{
		ContentType: objInfo.ContentType,
	})

	return err
}

// Helper methods

// toVirtualFileStat converts minio.ObjectInfo to VirtualFileStat
func (sb *S3Backend) toVirtualFileStat(key string, objInfo minio.ObjectInfo) *data.VirtualFileStat {
	// Determine if it's a directory
	isDir := strings.HasSuffix(objInfo.Key, "/") || objInfo.ContentType == "application/x-directory"
	virtMode := data.VirtualFileMode(0644)

	if isDir {
		virtMode = data.ModeDir | 0755
		// Remove trailing slash from key for consistency
		key = strings.TrimSuffix(key, "/")
	}

	return &data.VirtualFileStat{
		Key:        key,
		Size:       objInfo.Size,
		Mode:       virtMode,
		ModifyTime: objInfo.LastModified,
		CreateTime: objInfo.LastModified, // S3 doesn't track creation time separately
		ETag:       objInfo.ETag,
	}
}
