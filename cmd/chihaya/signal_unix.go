//go:build darwin || freebsd || linux || netbsd || openbsd || dragonfly || solaris
// +build darwin freebsd linux netbsd openbsd dragonfly solaris

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func makeReloadChan() <-chan os.Signal {
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGUSR1)
	return reload
}
