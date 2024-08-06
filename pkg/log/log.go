// Package log adds a thin wrapper around zerolog to improve logging performance.
//
// Root logger (called by log.Info, log.Warn etc.) uses global zerolog.Logger instance
// until ConfigureLogger called. Any child logger created with NewLogger will not
// produce any events until logger configured, so any function which uses child
// logger will come stuck because of root initialization synchronization.
package log

import (
	"io"
	"os"
	"strings"
	"sync"

	// needs for async file logging
	_ "code.cloudfoundry.org/go-diodes"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	zl "github.com/rs/zerolog/log"
)

var (
	root        = zl.Logger
	rootMu      = sync.Mutex{}
	customOut   io.WriteCloser
	customOutMu = sync.Mutex{}
)

// ConfigureLogger initializes root and all child loggers.
// NOTE: this function MUST be called before any child log call
//
//	otherwise any goroutine, which uses logger will wait logger initialization
func ConfigureLogger(output, level string, formatted, colored bool) (err error) {
	lvl := zerolog.WarnLevel
	output = strings.ToLower(output)
	var w io.Writer
	switch output {
	case "stderr", "":
		w = os.Stderr
	case "stdout":
		w = os.Stdout
	default:
		if w, err = os.OpenFile(output, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600); err == nil {
			customOutMu.Lock()
			defer customOutMu.Unlock()
			customOut = diode.NewWriter(w, 1000, 0, func(missed int) {
				zl.Warn().Int("count", missed).Msg("Logger dropped messages")
			})
			w = customOut
		} else {
			return err
		}
	}
	if formatted {
		w = zerolog.ConsoleWriter{
			Out:        w,
			NoColor:    !colored,
			TimeFormat: "2006-01-02 15:04:05.999",
		}
	}
	if len(level) > 0 {
		if logLevel, err := zerolog.ParseLevel(strings.ToLower(level)); err == nil {
			lvl = logLevel
		} else {
			return err
		}
	}
	rootMu.Lock()
	defer rootMu.Unlock()
	root = zerolog.New(w).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(lvl)
	return nil
}

// Logger is the holder for zerolog.Logger which
// waits until root logger initialized to prevent
// mixed logging format and output
type Logger struct {
	comp   string
	zlOnce sync.Once
	zerolog.Logger
}

func (l *Logger) init() {
	l.zlOnce.Do(func() {
		l.Logger = root.With().Str("component", l.comp).Logger()
	})
}

// ==== copied from zerolog ====

// Trace starts a new message with trace level.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Trace() *zerolog.Event {
	l.init()
	return l.Logger.Trace()
}

// Debug starts a new message with debug level.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Debug() *zerolog.Event {
	l.init()
	return l.Logger.Debug()
}

// Info starts a new message with info level.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Info() *zerolog.Event {
	l.init()
	return l.Logger.Info()
}

// Warn starts a new message with warn level.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Warn() *zerolog.Event {
	l.init()
	return l.Logger.Warn()
}

// Error starts a new message with error level.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Error() *zerolog.Event {
	l.init()
	return l.Logger.Error()
}

// Err starts a new message with error level with err as a field if not nil or
// with info level if err is nil.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Err(err error) *zerolog.Event {
	l.init()
	return l.Logger.Err(err)
}

// Fatal starts a new message with fatal level. The os.Exit(1) function
// is called by the Msg method, which terminates the program immediately.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Fatal() *zerolog.Event {
	l.init()
	return l.Logger.Fatal()
}

// Panic starts a new message with panic level. The panic() function
// is called by the Msg method, which stops the ordinary flow of a goroutine.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Panic() *zerolog.Event {
	l.init()
	return l.Logger.Panic()
}

// WithLevel starts a new message with level. Unlike Fatal and Panic
// methods, WithLevel does not terminate the program or stop the ordinary
// flow of a gourotine when used with their respective levels.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) WithLevel(level zerolog.Level) *zerolog.Event {
	l.init()
	return l.Logger.WithLevel(level)
}

// Log starts a new message with no level. Setting GlobalLevel to Disabled
// will still disable events produced by this method.
//
// You must call Msg on the returned event in order to send the event.
func (l *Logger) Log() *zerolog.Event {
	l.init()
	return l.Logger.Log()
}

// Print sends a log event using debug level and no extra field.
// Arguments are handled in the manner of fmt.Print.
func (l *Logger) Print(v ...any) {
	l.init()
	l.Logger.Print(v...)
}

// Printf sends a log event using debug level and no extra field.
// Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Printf(format string, v ...any) {
	l.init()
	l.Logger.Printf(format, v...)
}

// Write implements the io.Writer interface. This is useful to set as a writer
// for the standard library log.
func (l *Logger) Write(p []byte) (n int, err error) {
	l.init()
	return l.Logger.Write(p)
}

// Err starts a new message with error level with err as a field if not nil or
// with info level if err is nil.
//
// You must call Msg on the returned event in order to send the event.
func Err(err error) *zerolog.Event {
	return root.Err(err)
}

// Trace starts a new message with trace level.
//
// You must call Msg on the returned event in order to send the event.
func Trace() *zerolog.Event {
	return root.Trace()
}

// Debug starts a new message with debug level.
//
// You must call Msg on the returned event in order to send the event.
func Debug() *zerolog.Event {
	return root.Debug()
}

// Info starts a new message with info level.
//
// You must call Msg on the returned event in order to send the event.
func Info() *zerolog.Event {
	return root.Info()
}

// Warn starts a new message with warn level.
//
// You must call Msg on the returned event in order to send the event.
func Warn() *zerolog.Event {
	return root.Warn()
}

// Error starts a new message with error level.
//
// You must call Msg on the returned event in order to send the event.
func Error() *zerolog.Event {
	return root.Error()
}

// Fatal starts a new message with fatal level. The os.Exit(1) function
// is called by the Msg method.
//
// You must call Msg on the returned event in order to send the event.
func Fatal() *zerolog.Event {
	return root.Fatal()
}

// Panic starts a new message with panic level. The message is also sent
// to the panic function.
//
// You must call Msg on the returned event in order to send the event.
func Panic() *zerolog.Event {
	return root.Panic()
}

// WithLevel starts a new message with level.
//
// You must call Msg on the returned event in order to send the event.
func WithLevel(level zerolog.Level) *zerolog.Event {
	return root.WithLevel(level)
}

// Log starts a new message with no level. Setting zerolog.GlobalLevel to
// zerolog.Disabled will still disable events produced by this method.
//
// You must call Msg on the returned event in order to send the event.
func Log() *zerolog.Event {
	return root.Log()
}

// Print sends a log event using debug level and no extra field.
// Arguments are handled in the manner of fmt.Print.
func Print(v ...any) {
	root.Print(v...)
}

// Printf sends a log event using debug level and no extra field.
// Arguments are handled in the manner of fmt.Printf.
func Printf(format string, v ...any) {
	root.Printf(format, v...)
}

// Close closes custom output writer if it configured
func Close() {
	customOutMu.Lock()
	defer customOutMu.Unlock()
	if customOut != nil {
		_ = customOut.Close()
		customOut = nil
	}
}

// NewLogger creates child logger with specified component name
// NOTE: root logger MUST be initialized with ConfigureLogger
//
//	before any logger call
func NewLogger(component string) *Logger {
	return &Logger{comp: component}
}
