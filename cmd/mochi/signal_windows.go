//go:build windows

package main

import (
	"os"
	"syscall"
)

// ReloadSignals are the signals that the current OS will send to the process
// when a configuration reload is requested.
var ReloadSignals = []os.Signal{
	syscall.SIGHUP,
}
