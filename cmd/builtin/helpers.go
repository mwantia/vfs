package builtin

import (
	"fmt"
	"strconv"
	"strings"
)

func formatSize(bytes int64, humanReadable bool) string {
	if !humanReadable {
		return fmt.Sprintf("%12d", bytes)
	}

	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%9.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%9.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%9.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%10d B", bytes)
	}
}

// getFileType returns a human-readable file type
func getFileType(mode interface {
	IsDir() bool
	IsRegular() bool
}) string {
	if mode.IsDir() {
		return "directory"
	}
	if mode.IsRegular() {
		return "regular file"
	}
	return "other"
}

// parseSize parses a size string like "1mb", "256kb", "10gb" to bytes.
// Supports: B, KB, MB, GB (case-insensitive).
// Returns 0 and error if parsing fails.
func parseSize(sizeStr string) (int64, error) {
	const (
		B  = 1
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	sizeStr = strings.TrimSpace(strings.ToLower(sizeStr))
	if sizeStr == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Find where the number ends and unit begins
	i := 0
	for i < len(sizeStr) && (sizeStr[i] >= '0' && sizeStr[i] <= '9' || sizeStr[i] == '.') {
		i++
	}

	if i == 0 {
		return 0, fmt.Errorf("no numeric value in size string: %s", sizeStr)
	}

	numberStr := sizeStr[:i]
	unit := strings.TrimSpace(sizeStr[i:])

	// Parse the numeric value
	value, err := strconv.ParseFloat(numberStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number in size string: %s", sizeStr)
	}

	// Determine multiplier based on unit
	var multiplier int64
	switch unit {
	case "", "b":
		multiplier = B
	case "k", "kb":
		multiplier = KB
	case "m", "mb":
		multiplier = MB
	case "g", "gb":
		multiplier = GB
	default:
		return 0, fmt.Errorf("unknown size unit: %s (supported: B, KB, MB, GB)", unit)
	}

	return int64(value * float64(multiplier)), nil
}
