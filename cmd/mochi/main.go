package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	l "github.com/sot-tech/mochi/pkg/log"
)

const (
	logOutArg    = "logOut"
	logLevelArg  = "logLevel"
	logPrettyArg = "logPretty"
	logColorsArg = "logColored"
	configArg    = "config"
)

func main() {
	var s Server

	logOut := flag.String(logOutArg, "stderr", "output for logging, might be 'stderr', 'stdout' or file path")
	logLevel := flag.String(logLevelArg, "warn", "logging level: trace, debug, info, warn, error, fatal, panic")
	logPretty := flag.Bool(logPrettyArg, false, "enable log pretty print. used only if 'logOut' set to 'stdout' or 'stderr'. if not set, log outputs json")
	logColored := flag.Bool(logColorsArg, runtime.GOOS == "windows", "enable log coloring. used only if set 'logPretty'")
	configPath := flag.String(configArg, "/etc/mochi.yaml", "location of configuration file")
	flag.Parse()

	if err := l.ConfigureLogger(*logOut, *logLevel, *logPretty, *logColored); err != nil {
		log.Fatal("unable to configure logger: ", err)
	}

	if err := s.Run(*configPath); err != nil {
		log.Fatal("unable to start server: ", err)
	}
	defer s.Dispose()
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}
