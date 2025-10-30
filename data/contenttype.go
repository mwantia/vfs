package data

import (
	"path/filepath"
	"strings"
)

type ContentType string

const (
	ContentTypeTextPlain         = "text/plain"
	ContentTypeTextHTML          = "text/html"
	ContentTypeTextCSS           = "text/css"
	ContentTypeTextJavaScript    = "text/javascript"
	ContentTypeTextCSV           = "text/csv"
	ContentTypeImageJPEG         = "image/jpeg"
	ContentTypeImagePNG          = "image/png"
	ContentTypeImageGIF          = "image/gif"
	ContentTypeImageWebP         = "image/webp"
	ContentTypeImageSVGXML       = "image/svg+xml"
	ContentTypeAudioMpeg         = "audio/mpeg"
	ContentTypeAudioWAV          = "audio/wav"
	ContentTypeAudioOGG          = "audio/ogg"
	ContentTypeAudioWebM         = "audio/webm"
	ContentTypeVideoMP4          = "video/mp4"
	ContentTypeVideoWebM         = "video/webm"
	ContentTypeVideoQuickTime    = "video/quicktime"
	ContentTypeApplicationPDF    = "application/pdf"
	ContentTypeApplicationZip    = "application/zip"
	ContentTypeApplicationGZip   = "application/gzip"
	ContentTypeApplicationXTar   = "application/x-tar"
	ContentTypeApplicationJson   = "application/json"
	ContentTypeApplicationXML    = "application/xml"
	ContentTypeApplicationStream = "application/octet-stream"
	ContentTypeApplicationCustom = "application/x-custom"
)

// ExtensionToMIME maps file extensions to MIME types
var ExtensionToMIME = map[string]ContentType{
	".txt":  ContentTypeTextPlain,
	".html": ContentTypeTextHTML,
	".css":  ContentTypeTextCSS,
	".js":   ContentTypeTextJavaScript,
	".csv":  ContentTypeTextCSV,
	".jpg":  ContentTypeImageJPEG,
	".jpeg": ContentTypeImageJPEG,
	".png":  ContentTypeImagePNG,
	".gif":  ContentTypeImageGIF,
	".webp": ContentTypeImageWebP,
	".svg":  ContentTypeImageSVGXML,
	".mp3":  ContentTypeAudioMpeg,
	".wav":  ContentTypeAudioWAV,
	".ogg":  ContentTypeAudioOGG,
	".mp4":  ContentTypeVideoMP4,
	".webm": ContentTypeVideoWebM,
	".pdf":  ContentTypeApplicationPDF,
	".zip":  ContentTypeApplicationZip,
	".gz":   ContentTypeApplicationGZip,
	".tar":  ContentTypeApplicationXTar,
	".json": ContentTypeApplicationJson,
	".xml":  ContentTypeApplicationXML,
}

// MIMEToExtension is the reverse mapping (first/primary extension only)
var MIMEToExtension = map[ContentType]string{
	ContentTypeTextPlain:       ".txt",
	ContentTypeTextHTML:        ".html",
	ContentTypeTextCSS:         ".css",
	ContentTypeTextJavaScript:  ".js",
	ContentTypeTextCSV:         ".csv",
	ContentTypeImageJPEG:       ".jpg",
	ContentTypeImagePNG:        ".png",
	ContentTypeImageGIF:        ".gif",
	ContentTypeImageWebP:       ".webp",
	ContentTypeImageSVGXML:     ".svg",
	ContentTypeAudioMpeg:       ".mp3",
	ContentTypeAudioWAV:        ".wav",
	ContentTypeAudioOGG:        ".ogg",
	ContentTypeVideoMP4:        ".mp4",
	ContentTypeVideoWebM:       ".webm",
	ContentTypeApplicationPDF:  ".pdf",
	ContentTypeApplicationZip:  ".zip",
	ContentTypeApplicationGZip: ".gz",
	ContentTypeApplicationXTar: ".tar",
	ContentTypeApplicationJson: ".json",
	ContentTypeApplicationXML:  ".xml",
}

// GetMIMEType returns the MIME type for a file extension
func GetMIMEType(path string) ContentType {
	// Extract extension
	ext := strings.ToLower(filepath.Ext(path))

	if mimeType, exists := ExtensionToMIME[ext]; exists {
		return mimeType
	}

	// Default to octet-stream for unknown types
	return ContentTypeApplicationStream
}

// GetExtension returns the primary extension for a MIME type
func GetExtension(mimeType ContentType) string {
	if ext, exists := MIMEToExtension[mimeType]; exists {
		return ext
	}

	// Default to empty string for unknown types
	return ""
}
