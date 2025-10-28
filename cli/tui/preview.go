package tui

import (
	"encoding/hex"
	"fmt"
	"image"
	"image/color"

	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/eliukblau/pixterm/pkg/ansimage"
	"github.com/mwantia/vfs/data"
	"golang.org/x/image/draw"
)

// PreviewType represents how a file should be previewed
type PreviewType int

const (
	PreviewText PreviewType = iota
	PreviewImage
	PreviewBinary
	PreviewUnsupported
)

// FileTypeInfo contains information about how to preview a file
type FileTypeInfo struct {
	Type        PreviewType
	Description string
}

// DetectFileType determines the appropriate preview type for a file
func DetectFileType(filename string) FileTypeInfo {
	ext := strings.ToLower(filepath.Ext(filename))

	// Image files
	imageExts := map[string]bool{}
	if imageExts[ext] {
		return FileTypeInfo{
			Type:        PreviewImage,
			Description: "Image file",
		}
	}

	// Text files
	textExts := map[string]bool{
		".txt": true, ".md": true, ".go": true, ".js": true,
		".py": true, ".java": true, ".c": true, ".cpp": true,
		".h": true, ".hpp": true, ".rs": true, ".sh": true,
		".bash": true, ".zsh": true, ".fish": true,
		".json": true, ".xml": true, ".yaml": true, ".yml": true,
		".toml": true, ".ini": true, ".cfg": true, ".conf": true,
		".html": true, ".css": true, ".scss": true, ".sass": true,
		".sql": true, ".log": true, ".csv": true, ".tsv": true,
		".gitignore": true, ".dockerfile": true, ".env": true,
	}
	if textExts[ext] {
		return FileTypeInfo{
			Type:        PreviewText,
			Description: "Text file",
		}
	}

	// Binary files that shouldn't be previewed as text
	binaryExts := map[string]bool{
		".zip": true, ".gz": true, ".tar": true, ".bz2": true,
		".7z": true, ".rar": true, ".xz": true,
		".exe": true, ".dll": true, ".so": true, ".dylib": true,
		".bin": true, ".dat": true, ".db": true, ".sqlite": true,
		".pdf": true, ".doc": true, ".docx": true, ".xls": true,
		".xlsx": true, ".ppt": true, ".pptx": true,
		".mp3": true, ".mp4": true, ".avi": true, ".mkv": true,
		".wav": true, ".flac": true, ".ogg": true,

		".png": true, ".jpg": true, ".jpeg": true,
		".gif": true, ".bmp": true, ".webp": true,
	}
	if binaryExts[ext] {
		return FileTypeInfo{
			Type:        PreviewBinary,
			Description: "Binary file",
		}
	}

	// Files without extension or unknown - try to detect
	if ext == "" || ext == filename {
		return FileTypeInfo{
			Type:        PreviewText, // Try text, will be validated
			Description: "Unknown type",
		}
	}

	// Default to text for unknown extensions
	return FileTypeInfo{
		Type:        PreviewText,
		Description: "Unknown text file",
	}
}

// isValidUTF8 checks if data appears to be valid UTF-8 text
func isValidUTF8(data []byte) bool {
	// Check if it's valid UTF-8
	if !utf8.Valid(data) {
		return false
	}

	// Check for control characters (except common ones)
	controlCharCount := 0
	for _, b := range data {
		// Allow: tab (9), newline (10), carriage return (13)
		if b < 32 && b != 9 && b != 10 && b != 13 {
			controlCharCount++
		}
	}

	// If more than 5% control characters, likely binary
	return float64(controlCharCount)/float64(len(data)) < 0.05
}

// GenerateTextPreview creates a text preview of a file
func (a *VFSAdapter) GenerateTextPreview(path string, maxBytes int) (string, error) {
	file, err := a.vfs.OpenFile(a.ctx, path, data.AccessModeRead)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Read up to maxBytes
	buf := make([]byte, maxBytes)
	n, err := io.ReadFull(file, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	buf = buf[:n]

	// Validate it's actually text
	if !isValidUTF8(buf) {
		return "[Binary file - cannot preview as text]", nil
	}

	return string(buf), nil
}

// GenerateImagePreview creates an ANSI art preview of an image
func (a *VFSAdapter) GenerateImagePreview(path string, previewWidth, previewHeight int) (string, error) {
	// First check file size to prevent loading huge images
	stat, err := a.vfs.StatMetadata(a.ctx, path)
	if err != nil {
		return "", fmt.Errorf("failed to stat image: %w", err)
	}

	// Skip images larger than 5MB - too slow to render
	const maxImageSize = 5 * 1024 * 1024
	if stat.Size > maxImageSize {
		return fmt.Sprintf("[Image too large to preview: %.1f MB]\n\nUse a dedicated image viewer for files > 5MB",
			float64(stat.Size)/(1024*1024)), nil
	}

	file, err := a.vfs.OpenFile(a.ctx, path, data.AccessModeRead)
	if err != nil {
		return "", fmt.Errorf("failed to open image: %w", err)
	}
	defer file.Close()

	// Decode image
	img, format, err := image.Decode(file)
	if err != nil {
		return "", fmt.Errorf("failed to decode image: %w", err)
	}

	maxHeight := 80
	maxWidth := 260

	bounds := img.Bounds()
	imgWidth := bounds.Dx()
	imgHeight := bounds.Dy()

	scale := min64(float64(maxWidth)/float64(imgWidth), float64(maxHeight)/float64(imgHeight))
	if scale > 1.0 {
		scale = 1.0 // don’t upscale
	}
	newW := int(float64(imgWidth) * float64(scale))
	newH := int(float64(imgHeight) * float64(scale))

	dst := image.NewRGBA(image.Rect(0, 0, newW, newH))
	draw.CatmullRom.Scale(dst, dst.Bounds(), img, bounds, draw.Over, nil)

	// Create ANSI image with calculated dimensions
	ansImg, err := ansimage.NewFromImage(dst, color.Transparent, ansimage.NoDithering)
	if err != nil {
		return "", fmt.Errorf("failed to create ANSI image: %w", err)
	}

	rendered := ansImg.Render()
	header := fmt.Sprintf("Image: %s format, %dx%d pixels\n\n", format, imgWidth, imgHeight)

	return header + rendered, nil
}

// GenerateBinaryPreview creates a hex dump preview of a binary file
func (a *VFSAdapter) GenerateBinaryPreview(path string, maxBytes int) (string, error) {
	file, err := a.vfs.OpenFile(a.ctx, path, data.AccessModeRead)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Get file info
	stat, err := a.vfs.StatMetadata(a.ctx, path)
	if err != nil {
		return "", err
	}

	// Read up to maxBytes
	buf := make([]byte, 512)
	n, err := io.ReadFull(file, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	buf = buf[:n]

	var preview strings.Builder
	preview.WriteString(fmt.Sprintf("Binary file: %s\n", filepath.Base(path)))
	preview.WriteString(fmt.Sprintf("Size: %d bytes\n\n", stat.Size))
	preview.WriteString("Hex dump (first 512 bytes):\n")
	preview.WriteString(strings.Repeat("-", 60))
	preview.WriteString("\n")

	// Limit hex dump to 512 bytes
	dumpSize := min(maxBytes, len(buf))
	dumper := hex.Dumper(&preview)
	dumper.Write(buf[:dumpSize])
	dumper.Close()

	if stat.Size > int64(dumpSize) {
		preview.WriteString("\n... (truncated)")
	}

	return preview.String(), nil
}

// GeneratePreview generates an appropriate preview for any file
func (a *VFSAdapter) GeneratePreview(path string, previewWidth, previewHeight int) (string, error) {
	fileInfo := DetectFileType(path)

	switch fileInfo.Type {
	case PreviewText:
		content, err := a.GenerateTextPreview(path, 10240) // 10KB
		if err != nil {
			return "", err
		}
		return content, nil

	case PreviewImage:
		// Reserve space for header and borders
		content, err := a.GenerateImagePreview(path, previewWidth, previewHeight)
		if err != nil {
			// If image rendering fails, fall back to binary preview
			return a.GenerateBinaryPreview(path, 1024)
		}
		return content, nil

	case PreviewBinary:
		return a.GenerateBinaryPreview(path, 1024) // 1KB hex dump

	case PreviewUnsupported:
		return fmt.Sprintf("[Cannot preview %s files]", fileInfo.Description), nil

	default:
		return "[Unknown file type]", nil
	}
}

func min64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
