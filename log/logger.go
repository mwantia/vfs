package log

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger struct {
	writer io.Writer

	Name  string
	Level LogLevel

	TimeFormat string
	File       string
	NoColor    bool
	JSON       bool
	NoTerminal bool
	Rotation   *LoggerRotation
}

type LoggerRotation struct {
	MaxSize    int
	MaxBackups int
	MaxAge     int
	Compress   bool
}

type logEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Service   string `json:"service,omitempty"`
	Message   string `json:"message"`
}

func NewLogger(name string, level LogLevel, file string, noTerminal bool) *Logger {
	l := &Logger{
		Name:       name,
		Level:      level,
		File:       file,
		NoTerminal: noTerminal,

		TimeFormat: "2006-01-02 15:04:05",
		Rotation: &LoggerRotation{
			MaxSize:    128,
			MaxBackups: 5,
			MaxAge:     16,
			Compress:   false,
		},
	}

	l.setupWriter()

	return l
}

func (l *Logger) setupWriter() {
	var writers []io.Writer

	if !l.NoTerminal {
		writers = append(writers, os.Stdout)
	}

	if l.File != "" {
		fileWriter := &lumberjack.Logger{
			Filename:   l.File,
			MaxSize:    l.Rotation.MaxSize,
			MaxBackups: l.Rotation.MaxBackups,
			MaxAge:     l.Rotation.MaxAge,
			Compress:   l.Rotation.Compress,
		}
		writers = append(writers, fileWriter)
	}

	if len(writers) == 0 {
		writers = append(writers, os.Stdout)
	}

	l.writer = io.MultiWriter(writers...)
}

func (l *Logger) log(level LogLevel, msg string, args ...any) {
	if level < l.Level {
		return
	}

	timestamp := time.Now().Format(l.TimeFormat)
	formattedMsg := fmt.Sprintf(msg, args...)

	if l.JSON {
		entry := logEntry{
			Timestamp: timestamp,
			Level:     level.String(),
			Message:   formattedMsg,
		}
		if l.Name != "" {
			entry.Service = l.Name
		}

		jsonBytes, _ := json.Marshal(entry)
		fmt.Fprintf(l.writer, "%s\n", jsonBytes)
	} else {
		prefix := fmt.Sprintf("[%s] %-5s", timestamp, level)
		if l.Name != "" {
			prefix = fmt.Sprintf("%s [%s]", prefix, l.Name)
		}

		if !l.NoTerminal && !l.NoColor {
			fmt.Fprintf(l.writer, "%s%s %s\033[0m\n", Color(level), prefix, formattedMsg)
		} else {
			fmt.Fprintf(l.writer, "%s %s\n", prefix, formattedMsg)
		}
	}

	if level == Fatal {
		os.Exit(1)
	}
}

func (l *Logger) Debug(msg string, args ...any) {
	l.log(Debug, msg, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.log(Info, msg, args...)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.log(Warn, msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.log(Error, msg, args...)
}

func (l *Logger) Fatal(msg string, args ...any) {
	l.log(Fatal, msg, args...)
}

func (l *Logger) Named(name string) *Logger {
	return &Logger{
		writer: l.writer, // Share the same writer

		Name:  fmt.Sprintf("%s/%s", l.Name, name),
		Level: l.Level,

		TimeFormat: l.TimeFormat,
		File:       l.File,
		NoColor:    l.NoColor,
		NoTerminal: l.NoTerminal,
		JSON:       l.JSON,
		Rotation:   l.Rotation,
	}
}
