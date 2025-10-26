package util

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Logger struct {
	name   string
	logger *log.Logger
}

func NewLogger(name string) *Logger {
	return &Logger{
		name:   name,
		logger: log.New(os.Stdout, "", 0),
	}
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.log("INFO", format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.log("ERROR", format, args...)
}

func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log("FATAL", format, args...)
	os.Exit(1)
}

func (l *Logger) log(level, format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] [%s] [%s] %s", timestamp, level, l.name, message)
}
