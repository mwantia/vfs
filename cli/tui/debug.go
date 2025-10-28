package tui

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	debugFile *os.File
	debugMu   sync.Mutex
)

// InitDebugLog opens the debug log file
func InitDebugLog() error {
	var err error
	debugFile, err = os.OpenFile("debug.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	DebugLog("=== Debug log started ===")
	return nil
}

// CloseDebugLog closes the debug log file
func CloseDebugLog() {
	if debugFile != nil {
		DebugLog("=== Debug log ended ===")
		debugFile.Close()
	}
}

// DebugLog writes a message to the debug log with timestamp
func DebugLog(format string, args ...interface{}) {
	if debugFile == nil {
		return
	}

	debugMu.Lock()
	defer debugMu.Unlock()

	timestamp := time.Now().Format("15:04:05.000")
	message := fmt.Sprintf(format, args...)
	fmt.Fprintf(debugFile, "[%s] %s\n", timestamp, message)
	debugFile.Sync() // Flush to disk immediately
}
