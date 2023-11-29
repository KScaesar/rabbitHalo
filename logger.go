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

	ShallowCopy() Logger
	SetLogLevel(level LogLevel)
	LogLevel() LogLevel
	WithCallDepth(externalDepth uint) Logger
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
	defaultLogger Logger = NewLogger(0, LogLevelDebug)
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

func NewLogger(externalDepth uint, level LogLevel) stdLogger {
	flag := log.Ltime | log.LUTC | log.Lmsgprefix | log.Lshortfile
	internalDepth := 2
	return stdLogger{
		debug:             log.New(os.Stdout, "[Debug]: ", flag),
		info:              log.New(os.Stdout, "[Info]: ", flag),
		warn:              log.New(os.Stderr, "[Warn]: ", flag),
		err:               log.New(os.Stderr, "[Error]: ", flag),
		fatal:             log.New(os.Stderr, "[Fatal]: ", flag),
		internalCallDepth: internalDepth + int(externalDepth),
		logLevel:          &level,
	}
}

type stdLogger struct {
	debug             *log.Logger
	info              *log.Logger
	warn              *log.Logger
	err               *log.Logger
	fatal             *log.Logger
	internalCallDepth int
	logLevel          *LogLevel
}

func (l stdLogger) Debug(format string, a ...interface{}) {
	if *l.logLevel > LogLevelDebug {
		return
	}
	l.debug.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l stdLogger) Info(format string, a ...interface{}) {
	if *l.logLevel > LogLevelInfo {
		return
	}
	l.info.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l stdLogger) Warn(format string, a ...interface{}) {
	if *l.logLevel > LogLevelWarn {
		return
	}
	l.warn.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l stdLogger) Error(format string, a ...interface{}) {
	if *l.logLevel > LogLevelError {
		return
	}
	l.err.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l stdLogger) Fatal(format string, a ...interface{}) {
	if *l.logLevel > LogLevelFatal {
		return
	}
	l.fatal.Output(l.internalCallDepth, fmt.Sprintf(format, a...))
}

func (l stdLogger) ShallowCopy() Logger {
	return l.shallowCopy()
}

func (l stdLogger) shallowCopy() stdLogger {
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

func (l stdLogger) SetLogLevel(level LogLevel) {
	*l.logLevel = level
}

func (l stdLogger) LogLevel() LogLevel {
	return *l.logLevel
}

func (l stdLogger) WithCallDepth(externalDepth uint) Logger {
	l2 := l.shallowCopy()
	l2.internalCallDepth += int(externalDepth)
	return l2
}
