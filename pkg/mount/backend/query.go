package backend

import (
	"sort"
	"strings"

	"github.com/mwantia/vfs/data"
)

type Query struct {
	// Prefix matches keys starting with this string
	// Example: "data/cache/" lists everything under that directory
	Prefix string `json:"prefix"`

	// Delimiter for hierarchical listing
	// "/" means only immediate children (stops at next slash)
	// "" or nil means recursive (all descendants)
	Delimiter string `json:"delimiter,omitempty"`

	// Query filter by content-type (e.g. "text/plain")
	ContentType *string `json:"content_type,omitempty"`

	// Filter by file type (directory, regular file, symlink, etc.)
	FilterType *data.FileType `json:"filter_type,omitempty"`

	// Object size filtering
	MinSize *int64 `json:"min_size,omitempty"`
	MaxSize *int64 `json:"max_size,omitempty"`

	// Match custom attributes (mount-specific metadata)
	AttributeMatch map[string]string `json:"attribute_match"`

	// Max results to return (0 = unlimited)
	Limit int `json:"limit"`

	// Skip this many results during pagination
	Offset int `json:"offset"`

	// ===== Sorting =====
	SortBy    SortField `json:"sort_by"`
	SortOrder SortOrder `json:"sort_order"`
}

type QueryResult struct {
	// List of all queried metadata candidates
	Candidates []*data.Metadata

	// Total matches before pagination
	TotalCount int

	// Whenever more results exist beyond total cound limit
	Paginating bool
}

type SortField string

const (
	SortByKey        SortField = "key"
	SortBySize       SortField = "size"
	SortByModifyTime SortField = "modify_time"
	SortByCreateTime SortField = "create_time"
)

type SortOrder string

const (
	SortAsc  SortOrder = "asc"
	SortDesc SortOrder = "desc"
)

func ApplyFilters(metas []*data.Metadata, query *Query) []*data.Metadata {
	filtered := make([]*data.Metadata, 0)
	for _, meta := range metas {
		contentType := string(meta.ContentType)
		// Content type query filter
		if query.ContentType != nil && !matchContentType(contentType, *query.ContentType) {
			continue
		}
		// File type query filter
		if query.FilterType != nil && meta.GetType() != *query.FilterType {
			continue
		}
		// File size query filter
		if query.MinSize != nil && meta.Size < *query.MinSize {
			continue
		}
		if query.MaxSize != nil && meta.Size > *query.MaxSize {
			continue
		}

		filtered = append(filtered, meta)
	}

	ApplySort(filtered, query.SortBy, query.SortOrder)

	return filtered
}

func ApplySort(metas []*data.Metadata, by SortField, order SortOrder) {
	if by != "" {
		sort.Slice(metas, func(i, j int) bool {
			switch by {
			case SortByKey:
				if order == SortDesc {
					return metas[i].Key > metas[j].Key
				}
				return metas[i].Key < metas[j].Key
			case SortBySize:
				if order == SortDesc {
					return metas[i].Size > metas[j].Size
				}
				return metas[i].Size < metas[j].Size
			case SortByModifyTime:
				if order == SortDesc {
					return metas[i].ModifyTime.After(metas[j].ModifyTime)
				}
				return metas[i].ModifyTime.Before(metas[j].ModifyTime)
			case SortByCreateTime:
				if order == SortDesc {
					return metas[i].CreateTime.After(metas[j].CreateTime)
				}
				return metas[i].CreateTime.Before(metas[j].CreateTime)
			default:
				return false
			}
		})
	}
}

// matchContentType checks if a content type matches a pattern with wildcard support.
// Supports wildcards like "image/*", "*/json", "*/*", or "*"
func matchContentType(contentType string, pattern string) bool {
	// Full wildcard
	if pattern == "*" || pattern == "*/*" {
		return true
	}

	// Exact match
	if contentType == pattern {
		return true
	}

	// Parse content type and pattern (format: type/subtype)
	contentParts := strings.Split(contentType, "/")
	patternParts := strings.Split(pattern, "/")

	// Different structure (e.g., comparing "text/plain" with "image")
	if len(contentParts) != len(patternParts) {
		return false
	}

	// Check each part with wildcard support
	for i := range patternParts {
		if patternParts[i] != "*" && patternParts[i] != contentParts[i] {
			return false
		}
	}

	return true
}
