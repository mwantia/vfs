package builtin

import "fmt"

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
