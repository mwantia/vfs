package tui

import (
	"fmt"
	"time"

	"github.com/mwantia/vfs/data"
)

// Entry represents a file or directory entry in the TUI
type Entry struct {
	Name     string
	Path     string
	Size     int64
	Mode     data.VirtualFileMode
	ModTime  time.Time
	IsDir    bool
	MimeType string
}

// DisplayName returns the name with appropriate indicator
func (e *Entry) DisplayName() string {
	if e.IsDir {
		return e.Name + "/"
	}
	return e.Name
}

// DisplaySize returns human-readable size
func (e *Entry) DisplaySize() string {
	// Check if it's a mount point first
	if e.Mode.IsMount() {
		return "<MNT>"
	}

	if e.IsDir {
		return "<DIR>"
	}

	const unit = 1024
	if e.Size < unit {
		return fmt.Sprintf("%d B", e.Size)
	}

	div, exp := int64(unit), 0
	for n := e.Size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(e.Size)/float64(div), "KMGTPE"[exp])
}

// DisplayMode returns file permissions as string
func (e *Entry) DisplayMode() string {
	return e.Mode.String()
}

// DisplayModTime returns formatted modification time
func (e *Entry) DisplayModTime() string {
	return e.ModTime.Format("2006-01-02 15:04:05")
}

// Icon returns an icon character based on file type (simple version)
func (e *Entry) Icon() string {
	// Check if it's a mount point first
	if e.Mode.IsMount() {
		return "💾"
	}

	if e.IsDir {
		return "📁"
	}

	// Simple icon mapping based on file extension
	switch {
	case isTextFile(e.Name):
		return "📄"
	case isImageFile(e.Name):
		return "🖼️"
	case isArchiveFile(e.Name):
		return "📦"
	case isCodeFile(e.Name):
		return "💻"
	default:
		return "📄"
	}
}

// Helper functions for file type detection
func isTextFile(name string) bool {
	exts := []string{".txt", ".md", ".log", ".conf", ".cfg", ".ini"}
	for _, ext := range exts {
		if len(name) > len(ext) && name[len(name)-len(ext):] == ext {
			return true
		}
	}
	return false
}

func isImageFile(name string) bool {
	exts := []string{".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp"}
	for _, ext := range exts {
		if len(name) > len(ext) && name[len(name)-len(ext):] == ext {
			return true
		}
	}
	return false
}

func isArchiveFile(name string) bool {
	exts := []string{".zip", ".tar", ".gz", ".bz2", ".7z", ".rar"}
	for _, ext := range exts {
		if len(name) > len(ext) && name[len(name)-len(ext):] == ext {
			return true
		}
	}
	return false
}

func isCodeFile(name string) bool {
	exts := []string{".go", ".js", ".ts", ".py", ".java", ".c", ".cpp", ".h", ".rs", ".rb", ".php"}
	for _, ext := range exts {
		if len(name) > len(ext) && name[len(name)-len(ext):] == ext {
			return true
		}
	}
	return false
}
