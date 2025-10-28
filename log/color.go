package log

func Color(l LogLevel) string {
	switch l {
	case Debug:
		return "\033[34m"
	case Info:
		return "\033[32m"
	case Warn:
		return "\033[33m"
	case Error:
		return "\033[31m"
	case Fatal:
		return "\033[35m"
	default:
		return "\033[0m"
	}
}
