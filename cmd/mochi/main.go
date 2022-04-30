package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sot-tech/mochi/frontend/http"
	"github.com/sot-tech/mochi/frontend/udp"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	_ "github.com/sot-tech/mochi/pkg/randseed"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/storage"
)

var e2eCmd *cobra.Command

// Run represents the state of a running instance of Conf.
type Run struct {
	configFilePath string
	storage        storage.PeerStorage
	logic          *middleware.Logic
	sg             *stop.Group
}

// NewRun runs an instance of Conf.
func NewRun(configFilePath string) (*Run, error) {
	r := &Run{
		configFilePath: configFilePath,
	}

	return r, r.Start(nil)
}

// Start begins an instance of Conf.
// It is optional to provide an instance of the peer store to avoid the
// creation of a new one.
func (r *Run) Start(ps storage.PeerStorage) error {
	configFile, err := ParseConfigFile(r.configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	cfg := configFile.Conf

	r.sg = stop.NewGroup()

	if len(cfg.MetricsAddr) > 0 {
		log.Info().Str("addr", cfg.MetricsAddr).Msg("starting metrics server")
		r.sg.Add(metrics.NewServer(cfg.MetricsAddr))
	} else {
		log.Info().Msg("metrics disabled because of empty address")
	}

	if ps == nil {
		log.Info().Str("name", cfg.Storage.Name).Msg("starting storage")
		ps, err = storage.NewStorage(cfg.Storage.Name, cfg.Storage.Config)
		if err != nil {
			return fmt.Errorf("failed to create storage: %w", err)
		}
		log.Info().Object("config", ps).Msg("started storage")
	}
	r.storage = ps

	preHooks, err := middleware.HooksFromHookConfigs(cfg.PreHooks, r.storage)
	if err != nil {
		return fmt.Errorf("failed to validate hook config: %w", err)
	}
	postHooks, err := middleware.HooksFromHookConfigs(cfg.PostHooks, r.storage)
	if err != nil {
		return fmt.Errorf("failed to validate hook config: %w", err)
	}

	r.logic = middleware.NewLogic(cfg.AnnounceInterval, cfg.MinAnnounceInterval, r.storage, preHooks, postHooks)

	if len(cfg.HTTPConfig) > 0 {
		log.Info().Object("config", cfg.HTTPConfig).Msg("starting HTTP frontend")
		httpFE, err := http.NewFrontend(r.logic, cfg.HTTPConfig)
		if err == nil {
			r.sg.Add(httpFE)
		} else if !errors.Is(err, conf.ErrNilConfigMap) {
			return err
		}
	}

	if len(cfg.UDPConfig) > 0 {
		log.Info().Object("config", cfg.HTTPConfig).Msg("starting UDP frontend")
		udpFE, err := udp.NewFrontend(r.logic, cfg.UDPConfig)
		if err == nil {
			r.sg.Add(udpFE)
		} else if !errors.Is(err, conf.ErrNilConfigMap) {
			return err
		}
	}

	return nil
}

func combineErrors(prefix string, errs []error) error {
	errStrs := make([]string, 0, len(errs))
	for _, err := range errs {
		errStrs = append(errStrs, err.Error())
	}

	return errors.New(prefix + ": " + strings.Join(errStrs, "; "))
}

// Stop shuts down an instance of Conf.
func (r *Run) Stop(keepPeerStore bool) (storage.PeerStorage, error) {
	log.Debug().Msg("stopping frontends and metrics server")
	if errs := r.sg.Stop().Wait(); len(errs) != 0 {
		return nil, combineErrors("failed while shutting down frontends", errs)
	}

	log.Debug().Msg("stopping logic")
	if errs := r.logic.Stop().Wait(); len(errs) != 0 {
		return nil, combineErrors("failed while shutting down middleware", errs)
	}

	if !keepPeerStore {
		log.Debug().Msg("stopping peer store")
		if errs := r.storage.Stop().Wait(); len(errs) != 0 {
			return nil, combineErrors("failed while shutting down peer store", errs)
		}
		r.storage = nil
	}

	return r.storage, nil
}

// RootRunCmdFunc implements a Cobra command that runs an instance of Conf
// and handles reloading and shutdown via process signals.
func RootRunCmdFunc(cmd *cobra.Command, _ []string) error {
	configFilePath, err := cmd.Flags().GetString(configArg)
	if err != nil {
		return err
	}

	r, err := NewRun(configFilePath)
	if err != nil {
		return err
	}

	shutdown, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	reload, _ := signal.NotifyContext(context.Background(), ReloadSignals...)

	for {
		select {
		case <-reload.Done():
			log.Info().Msg("reloading; received reload signal")
			peerStore, err := r.Stop(true)
			if err != nil {
				return err
			}

			if err := r.Start(peerStore); err != nil {
				return err
			}
		case <-shutdown.Done():
			log.Info().Msg("shutting down; received shutdown signal")
			if _, err := r.Stop(false); err != nil {
				return err
			}

			return nil
		}
	}
}

const (
	appName      = "mochi"
	logOutArg    = "logOut"
	logLevelArg  = "logLevel"
	logPrettyArg = "logPretty"
	logColorsArg = "logColored"
	configArg    = "config"
)

// configureLogger handles command line flags for the logger.
func configureLogger(cmd *cobra.Command, _ []string) (err error) {
	var out, lvl string
	var pretty, colored bool

	flags := cmd.Flags()

	out, err = flags.GetString(logOutArg)
	if err != nil {
		return err
	}

	lvl, err = flags.GetString(logLevelArg)
	if err != nil {
		return err
	}

	pretty, err = flags.GetBool(logPrettyArg)
	if err != nil {
		return err
	}

	colored, err = cmd.Flags().GetBool(logColorsArg)
	if err != nil {
		return err
	}

	return log.ConfigureLogger(out, lvl, pretty, colored)
}

func main() {
	rootCmd := &cobra.Command{
		Use:               appName,
		Short:             "BitTorrent Tracker",
		Long:              "A customizable, multi-protocol BitTorrent Tracker",
		PersistentPreRunE: configureLogger,
		RunE:              RootRunCmdFunc,
	}

	flags := rootCmd.PersistentFlags()

	flags.String(logOutArg, "", "output for logging, might be 'stderr', 'stdout' of file path. 'stderr' if not set")
	flags.String(logLevelArg, "info", "logging level (trace, debug, info, warn, error, fatal, panic). 'warn' if not set")
	flags.Bool(logPrettyArg, false, "enable log pretty print. used only if 'logOut' set to 'stdout' or 'stderr'. if not set, log outputs json)")
	flags.Bool(logColorsArg, runtime.GOOS == "windows", "enable log coloring. used only if set 'logPretty'")

	rootCmd.Flags().String("config", "/etc/mochi.yaml", "location of configuration file")

	if e2eCmd != nil {
		rootCmd.AddCommand(e2eCmd)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("failed while executing root command")
	}
}
