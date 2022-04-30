// Package log adds a thin wrapper around logrus to improve non-debug logging
// performance.
package log

import (
	"io"
	"os"
	"strings"
	"time"

	_ "code.cloudfoundry.org/go-diodes"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	zl "github.com/rs/zerolog/log"
)

var root = zl.Logger

func ConfigureLogger(output, level string, formatted, colored bool) (err error) {
	lvl := zerolog.WarnLevel
	output = strings.ToLower(output)
	var w io.Writer
	var stdAny bool
	switch output {
	case "stderr", "":
		w, stdAny = os.Stderr, true
	case "stdout":
		w, stdAny = os.Stdout, true
	default:
		if w, err = os.OpenFile(output, os.O_APPEND|os.O_CREATE, 0600); err == nil {
			w = diode.NewWriter(w, 1000, 10*time.Millisecond, func(missed int) {
				zl.Warn().Int("count", missed).Msg("Logger dropped messages")
			})

		} else {
			return err
		}
	}
	if stdAny && formatted {
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
	root = zerolog.New(w).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(lvl)
	return nil
}

func Debug() *zerolog.Event {
	return root.Debug()
}

func Info() *zerolog.Event {
	return root.Info()
}

func Warn() *zerolog.Event {
	return root.Warn()
}

func Error() *zerolog.Event {
	return root.Error()
}

func Fatal() *zerolog.Event {
	return root.Fatal()
}

func NewLogger(component string) zerolog.Logger {
	return root.With().Str("component", component).Logger()
}
