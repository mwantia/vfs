package s3

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/mwantia/vfs/data"
	"github.com/mwantia/vfs/mount/service"
)

func (s *S3ObjectStorageService) GetLifecycle() service.Lifecycle {
	return s.driver
}

func (s *S3ObjectStorageService) CreateObject(ctx context.Context, ns, key string, mode data.FileMode) (*data.FileStat, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	bucket := s.driver.cfg.BucketName
	named := s.buildNamedKey(ns, key)
	now := time.Now()

	// Check if object already exists
	_, err := s.driver.client.StatObject(ctx, bucket, named, minio.StatObjectOptions{})
	if err == nil {
		return nil, data.ErrExist
	}

	// For directories: create explicit marker with trailing "/"
	// This allows empty directories to exist (like Consul pattern)
	if mode.IsDir() {
		dirKey := named
		if !strings.HasSuffix(dirKey, "/") {
			dirKey += "/"
		}

		_, err = s.driver.client.PutObject(ctx, bucket, dirKey, strings.NewReader(""), 0, minio.PutObjectOptions{})
		if err != nil {
			return nil, err
		}

		return &data.FileStat{
			Key:        key,
			Mode:       mode,
			Size:       0,
			CreateTime: now,
			ModifyTime: now,
		}, nil
	}

	// For files: create empty object
	contentType := data.GetMIMEType(key)
	opts := minio.PutObjectOptions{
		ContentType: string(contentType),
	}

	_, err = s.driver.client.PutObject(
		ctx,
		s.driver.cfg.BucketName,
		named,
		strings.NewReader(""),
		0,
		opts,
	)
	if err != nil {
		return nil, err
	}

	return &data.FileStat{
		Key:         key,
		Mode:        mode,
		Size:        0,
		CreateTime:  now,
		ModifyTime:  now,
		ContentType: contentType,
	}, nil
}

func (s *S3ObjectStorageService) ReadObject(ctx context.Context, ns, key string, offset int64, buffer []byte) (int, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	bucket := s.driver.cfg.BucketName
	named := s.buildNamedKey(ns, key)

	// Get object info to check if it exists and is not a directory
	objInfo, err := s.driver.client.StatObject(ctx, bucket, named, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			// Check if it's an implicit directory
			prefix := named + "/"
			objects := s.driver.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				Prefix:  prefix,
				MaxKeys: 1,
			})

			for range objects {
				// Has children - it's an implicit directory
				return 0, data.ErrIsDirectory
			}
			return 0, data.ErrNotExist
		}
		return 0, err
	}

	// Check if it's a directory (trailing slash)
	if strings.HasSuffix(objInfo.Key, "/") {
		return 0, data.ErrIsDirectory
	}

	// If offset is beyond file size, return EOF
	if offset >= objInfo.Size {
		return 0, io.EOF
	}

	// Read object with offset
	opts := minio.GetObjectOptions{}
	if offset > 0 {
		// Calculate end position, clamped to file size
		endPos := offset + int64(len(buffer)) - 1
		if endPos >= objInfo.Size {
			endPos = objInfo.Size - 1
		}
		if err := opts.SetRange(offset, endPos); err != nil {
			return 0, err
		}
	}

	object, err := s.driver.client.GetObject(ctx, bucket, named, opts)
	if err != nil {
		return 0, err
	}
	defer object.Close()

	n, err := io.ReadFull(object, buffer)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return n, err
	}

	return n, nil
}

func (s *S3ObjectStorageService) WriteObject(ctx context.Context, ns, key string, offset int64, buffer []byte) (int, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	bucket := s.driver.cfg.BucketName
	named := s.buildNamedKey(ns, key)

	// S3 doesn't support partial writes - we need to read-modify-write
	// First, check if object exists
	objInfo, err := s.driver.client.StatObject(ctx, bucket, named, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return 0, data.ErrNotExist
		}
		return 0, err
	}

	// Check if it's a directory
	if strings.HasSuffix(objInfo.Key, "/") {
		return 0, data.ErrIsDirectory
	}

	// Read existing content
	var existingData []byte
	if objInfo.Size > 0 {
		object, err := s.driver.client.GetObject(ctx, bucket, named, minio.GetObjectOptions{})
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
	writeEnd := offset + int64(len(buffer))
	newSize := max(writeEnd, int64(len(existingData)))

	// Create new buffer
	newData := make([]byte, newSize)
	if len(existingData) > 0 {
		copy(newData, existingData)
	}

	// Write new data at offset
	copy(newData[offset:], buffer)

	reader := strings.NewReader(string(newData))
	// Upload the modified object (preserve content type)
	if _, err = s.driver.client.PutObject(ctx, bucket, named, reader, int64(len(newData)), minio.PutObjectOptions{
		ContentType: objInfo.ContentType,
	}); err != nil {
		return 0, err
	}

	return len(buffer), nil
}

func (s *S3ObjectStorageService) DeleteObject(ctx context.Context, ns, key string, force bool) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	bucket := s.driver.cfg.BucketName
	named := s.buildNamedKey(ns, key)

	// Try to stat the object
	objInfo, err := s.driver.client.StatObject(ctx, bucket, named, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			// Check if it's an implicit directory
			prefix := named + "/"
			objects := s.driver.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				Prefix:  prefix,
				MaxKeys: 1,
			})

			hasChildren := false
			for range objects {
				hasChildren = true
				break
			}

			if !hasChildren {
				return data.ErrNotExist
			}

			// It's an implicit directory
			if !force {
				return data.ErrIsDirectory
			}

			// Force delete all children (recursive)
			childObjects := s.driver.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				Prefix:    prefix,
				Recursive: true,
			})

			for object := range childObjects {
				if object.Err != nil {
					return object.Err
				}

				if err := s.driver.client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
					return err
				}
			}

			return nil
		}

		return err
	}

	// Check if it's a directory (explicit marker with trailing /)
	if strings.HasSuffix(objInfo.Key, "/") {
		if !force {
			return data.ErrIsDirectory
		}

		// Delete directory marker and all children
		prefix := named + "/"
		objects := s.driver.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		})

		for object := range objects {
			if object.Err != nil {
				return object.Err
			}

			if err := s.driver.client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
				return err
			}
		}

		// Delete the directory marker itself
		return s.driver.client.RemoveObject(ctx, bucket, named, minio.RemoveObjectOptions{})
	}

	// Delete single file
	return s.driver.client.RemoveObject(ctx, bucket, named, minio.RemoveObjectOptions{})
}

func (s *S3ObjectStorageService) ListObjects(ctx context.Context, ns, key string) ([]*data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	bucket := s.driver.cfg.BucketName
	named := s.buildNamedKey(ns, key)

	// Build prefix for listing
	prefix := named
	if key != "" {
		prefix += "/"
	}

	// List objects with Recursive: false to get direct children only
	objects := s.driver.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false, // Direct children only
	})

	var stats []*data.FileStat
	seen := make(map[string]bool) // Deduplicate

	for object := range objects {
		if object.Err != nil {
			return nil, object.Err
		}

		// Get relative path (remove prefix to get just the basename)
		relPath := strings.TrimPrefix(object.Key, prefix)
		if relPath == "" {
			continue // Skip directory object itself
		}

		// Remove trailing slash for directory names
		relPath = strings.TrimSuffix(relPath, "/")
		if seen[relPath] {
			continue
		}
		seen[relPath] = true

		// Construct absolute key: parent path + basename
		absoluteKey := key
		if absoluteKey != "" {
			absoluteKey += "/"
		}
		absoluteKey += relPath

		mode := data.FileMode(0644)
		if strings.HasSuffix(object.Key, "/") {
			mode = data.ModeDir | 0755
		}

		stats = append(stats, &data.FileStat{
			Key:         absoluteKey,
			Size:        object.Size,
			Mode:        mode,
			ModifyTime:  object.LastModified,
			CreateTime:  object.LastModified,
			ContentType: data.ContentType(object.ContentType),
			ETag:        object.ETag,
		})
	}

	// If no children found, check if the directory itself exists before returning ErrNotExist
	if len(stats) == 0 && key != "" {
		// Check for explicit directory marker
		dirKey := named
		if !strings.HasSuffix(dirKey, "/") {
			dirKey += "/"
		}

		if _, err := s.driver.client.StatObject(ctx, bucket, dirKey, minio.StatObjectOptions{}); err == nil {
			// Directory marker exists, return empty list
			return stats, nil
		}

		// Directory doesn't exist
		return nil, data.ErrNotExist
	}

	return stats, nil
}

func (s *S3ObjectStorageService) HeadObject(ctx context.Context, ns, key string) (*data.FileStat, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()

	bucket := s.driver.cfg.BucketName
	named := s.buildNamedKey(ns, key)

	// Try to stat the object directly
	objInfo, err := s.driver.client.StatObject(ctx, bucket, named, minio.StatObjectOptions{})
	if err == nil {
		return objectInfoToFileStat(key, objInfo), nil
	}

	// Object doesn't exist - check if it's a directory (explicit marker or implicit)
	errResponse := minio.ToErrorResponse(err)
	if errResponse.Code == "NoSuchKey" {
		// Check for explicit directory marker (with trailing "/")
		dirKey := named
		if !strings.HasSuffix(dirKey, "/") {
			dirKey += "/"
		}

		objInfo, err = s.driver.client.StatObject(ctx, bucket, dirKey, minio.StatObjectOptions{})
		if err == nil {
			// Found explicit directory marker
			return &data.FileStat{
				Key:        key,
				Mode:       data.ModeDir | 0755,
				Size:       0,
				CreateTime: objInfo.LastModified,
				ModifyTime: objInfo.LastModified,
			}, nil
		}

		// Check for implicit directory (has children with prefix)
		prefix := named + "/"
		objects := s.driver.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:  prefix,
			MaxKeys: 1,
		})

		for range objects {
			// Has children - it's an implicit directory
			now := time.Now()
			return &data.FileStat{
				Key:        key,
				Mode:       data.ModeDir | 0755,
				Size:       0,
				CreateTime: now,
				ModifyTime: now,
			}, nil
		}

		return nil, data.ErrNotExist
	}

	return nil, err
}

func (s *S3ObjectStorageService) TruncateObject(ctx context.Context, ns, key string, size int64) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()

	bucket := s.driver.cfg.BucketName
	named := s.buildNamedKey(ns, key)

	// Check if object exists
	objInfo, err := s.driver.client.StatObject(ctx, bucket, named, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return data.ErrNotExist
		}
		return err
	}

	// Check if it's a directory
	if strings.HasSuffix(objInfo.Key, "/") {
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
			object, err := s.driver.client.GetObject(ctx, bucket, named, minio.GetObjectOptions{})
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

	reader := strings.NewReader(string(newData))
	// Upload the truncated/expanded object (preserve content type)
	_, err = s.driver.client.PutObject(ctx, bucket, named, reader, int64(len(newData)), minio.PutObjectOptions{
		ContentType: objInfo.ContentType,
	})

	return err
}

// Helper functions

// buildNamedKey combines namespace and key with "/" separator for S3
// If key has leading "/", it strips it since S3 keys should be relative
func (s *S3ObjectStorageService) buildNamedKey(ns, key string) string {
	// Strip leading "/" from key if present (S3 keys are relative)
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// Use "/" separator for S3 (hierarchical namespace)
	return service.NamedKey(ns, key, "/")
}

// objectInfoToFileStat converts S3 ObjectInfo to FileStat
// Constructs FileStat from S3's native metadata: Size, LastModified, ETag, ContentType
func objectInfoToFileStat(key string, objInfo minio.ObjectInfo) *data.FileStat {
	// Determine file mode (default to regular file)
	mode := data.FileMode(0644)

	// Detect directories: trailing "/" indicates directory
	if strings.HasSuffix(objInfo.Key, "/") {
		mode = data.ModeDir | 0755
	}

	return &data.FileStat{
		Key:         key,
		Size:        objInfo.Size,
		Mode:        mode,
		CreateTime:  objInfo.LastModified, // S3 doesn't track creation time separately
		ModifyTime:  objInfo.LastModified,
		ContentType: data.ContentType(objInfo.ContentType),
		ETag:        objInfo.ETag,
	}
}
