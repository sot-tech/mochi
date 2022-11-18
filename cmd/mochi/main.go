// Package main contains entry point logic of MoChi server
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
	quickArg     = "quick"
)

func main() {
	var err error

	logOut := flag.String(logOutArg, "stderr", "output for logging, might be 'stderr', 'stdout' or file path")
	logLevel := flag.String(logLevelArg, "warn", "logging level: trace, debug, info, warn, error, fatal, panic")
	logPretty := flag.Bool(logPrettyArg, false, "enable log pretty print. used only if 'logOut' set to 'stdout' or 'stderr'. if not set, log outputs json")
	logColored := flag.Bool(logColorsArg, runtime.GOOS == "windows", "enable log coloring. used only if set 'logPretty'")
	configPath := flag.String(configArg, "/etc/mochi.yaml", "location of configuration file")
	quickStart := flag.Bool(quickArg, false, "start tracker with default configuration (all frontends, in-memory store, no hooks)")
	flag.Parse()

	if err = l.ConfigureLogger(*logOut, *logLevel, *logPretty, *logColored); err != nil {
		log.Fatal("unable to configure logger: ", err)
	}

	var cfg *Config
	if *quickStart {
		cfg = QuickConfig
	} else {
		cfg, err = ParseConfigFile(*configPath)
		if err != nil {
			log.Fatal("unable to read config file: ", err)
		}
	}
	var s Server

	if err = s.Run(cfg); err != nil {
		log.Fatal("unable to start server: ", err)
	}
	defer s.Shutdown()
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}
