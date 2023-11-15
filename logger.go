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

	SetLogLevel(level LogLevel) Logger
	LogLevel() LogLevel
	SetCallDepth(externalDepth uint) Logger
}

type LogLevel uint8

const (
	LogLevelDebug LogLevel = iota + 1
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelFatal
)

var (
	defaultLogger Logger = newLogger(0, LogLevelDebug)
)

func GlobalLogLevel() LogLevel {
	return defaultLogger.LogLevel()
}

func SetDefaultLogger(l Logger) {
	defaultLogger = l
}

func DefaultLogger() Logger {
	return defaultLogger
}

func newLogger(externalDepth uint, level LogLevel) *stdLogger {
	flag := log.Ltime | log.LUTC | log.Lmsgprefix | log.Lshortfile
	internalDepth := 2
	l := &stdLogger{
		debug:             log.New(os.Stderr, "[Debug]: ", flag),
		info:              log.New(os.Stderr, "[Info]: ", flag),
		warn:              log.New(os.Stderr, "[Warn]: ", flag),
		err:               log.New(os.Stderr, "[Error]: ", flag),
		fatal:             log.New(os.Stderr, "[Fatal]: ", flag),
		internalCallDepth: internalDepth + int(externalDepth),
		logLevel:          level,
	}
	return l
}

type stdLogger struct {
	debug             *log.Logger
	info              *log.Logger
	warn              *log.Logger
	err               *log.Logger
	fatal             *log.Logger
	internalCallDepth int
	logLevel          LogLevel
}

func (l *stdLogger) Debug(format string, a ...interface{}) {
	if l.logLevel > LogLevelDebug {
		return
	}
	l.debug.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) Info(format string, a ...interface{}) {
	if l.logLevel > LogLevelInfo {
		return
	}
	l.info.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) Warn(format string, a ...interface{}) {
	if l.logLevel > LogLevelWarn {
		return
	}
	l.warn.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) Error(format string, a ...interface{}) {
	if l.logLevel > LogLevelError {
		return
	}
	l.err.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) Fatal(format string, a ...interface{}) {
	if l.logLevel > LogLevelFatal {
		return
	}
	l.fatal.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l *stdLogger) shallowCopy() stdLogger {
	return stdLogger{
		debug:             l.debug,
		info:              l.info,
		warn:              l.warn,
		err:               l.err,
		fatal:             l.fatal,
		internalCallDepth: l.internalCallDepth,
		logLevel:          l.logLevel,
	}
}

func (l *stdLogger) SetLogLevel(level LogLevel) Logger {
	l2 := l.shallowCopy()
	l2.logLevel = level
	return &l2
}

func (l *stdLogger) LogLevel() LogLevel {
	return l.logLevel
}

func (l *stdLogger) SetCallDepth(externalDepth uint) Logger {
	l2 := l.shallowCopy()
	l2.internalCallDepth += int(externalDepth)
	return &l2
}
