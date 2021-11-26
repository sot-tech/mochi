//go:build windows
// +build windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func makeReloadChan() <-chan os.Signal {
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	return reload
}
