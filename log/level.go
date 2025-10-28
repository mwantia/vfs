package log

import "strings"

type LogLevel int

const (
	Debug LogLevel = iota
	Info
	Warn
	Error
	Fatal
)

func (l LogLevel) String() string {
	switch l {
	case Debug:
		return "DEBUG"
	case Info:
		return "INFO"
	case Warn:
		return "WARN"
	case Error:
		return "ERROR"
	case Fatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

func Parse(level string) LogLevel {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return Debug
	case "INFO":
		return Info
	case "WARN":
		return Warn
	case "ERROR":
		return Error
	case "FATAL":
		return Fatal
	default:
		panic("Invalid log_level defined...")
	}
}
