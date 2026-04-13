package aibot

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Logger interface {
	Debug(message string, args ...any)
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Error(message string, args ...any)
}

type DefaultLogger struct {
	prefix string
	logger *log.Logger
}

func NewDefaultLogger(prefix string) *DefaultLogger {
	return &DefaultLogger{
		prefix: prefix,
		logger: log.New(os.Stderr, "", 0),
	}
}

func (l *DefaultLogger) Debug(message string, args ...any) {
	l.log("DEBUG", message, args...)
}

func (l *DefaultLogger) Info(message string, args ...any) {
	l.log("INFO", message, args...)
}

func (l *DefaultLogger) Warn(message string, args ...any) {
	l.log("WARN", message, args...)
}

func (l *DefaultLogger) Error(message string, args ...any) {
	l.log("ERROR", message, args...)
}

func (l *DefaultLogger) log(level, message string, args ...any) {
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	l.logger.Printf("[%s] [%s] [%s] %s", time.Now().UTC().Format(time.RFC3339Nano), l.prefix, level, message)
}
