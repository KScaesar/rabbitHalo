package rabbitHalo

import (
	"fmt"
	"log"
	"os"
)

type Logger interface {
	Debug(format string, a ...interface{})
	Info(format string, a ...interface{})
	Warn(format string, a ...interface{})
	Error(format string, a ...interface{})
	Fatal(format string, a ...interface{})
	CallDepth(v int) Logger
}

var defaultLogger Logger = NewLogger(2, log.Ltime|log.Lshortfile|log.LUTC|log.Lmsgprefix)

func SetDefaultLogger(l Logger) {
	defaultLogger = l
}

func DefaultLogger() Logger {
	return defaultLogger
}

func NewLogger(callDepth int, flag int) *stdLogger {
	l := &stdLogger{
		debug:     log.New(os.Stderr, "Debug: ", flag),
		info:      log.New(os.Stderr, "Info: ", flag),
		warn:      log.New(os.Stderr, "Warn: ", flag),
		err:       log.New(os.Stderr, "Error: ", flag),
		fatal:     log.New(os.Stderr, "Fatal: ", flag),
		callDepth: callDepth,
	}
	return l
}

type stdLogger struct {
	debug     *log.Logger
	info      *log.Logger
	warn      *log.Logger
	err       *log.Logger
	fatal     *log.Logger
	callDepth int
}

func (l *stdLogger) Debug(format string, a ...interface{}) {
	l.debug.Output(l.callDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) Info(format string, a ...interface{}) {
	l.info.Output(l.callDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) Warn(format string, a ...interface{}) {
	l.warn.Output(l.callDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) Error(format string, a ...interface{}) {
	l.err.Output(l.callDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) Fatal(format string, a ...interface{}) {
	l.fatal.Output(l.callDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) CallDepth(v int) Logger {
	return &stdLogger{
		debug:     l.debug,
		info:      l.info,
		warn:      l.warn,
		err:       l.err,
		fatal:     l.fatal,
		callDepth: v,
	}
}
