package main

import (
	"errors"
	"fmt"

	"github.com/sot-tech/mochi/frontend/http"
	"github.com/sot-tech/mochi/frontend/udp"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/storage"
)

// Server represents the state of a running instance.
type Server struct {
	storage storage.PeerStorage
	logic   *middleware.Logic
	sg      *stop.Group
}

// Run begins an instance of Conf.
// It is optional to provide an instance of the peer store to avoid the
// creation of a new one.
func (r *Server) Run(configFilePath string) error {
	configFile, err := ParseConfigFile(configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	cfg := configFile.Conf

	r.sg = stop.NewGroup()

	if len(cfg.MetricsAddr) > 0 {
		log.Info().Str("address", cfg.MetricsAddr).Msg("starting metrics server")
		r.sg.Add(metrics.NewServer(cfg.MetricsAddr))
	} else {
		log.Info().Msg("metrics disabled because of empty address")
	}

	log.Info().Str("name", cfg.Storage.Name).Msg("starting storage")
	r.storage, err = storage.NewStorage(cfg.Storage.Name, cfg.Storage.Config)
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}
	log.Info().Object("config", r.storage).Msg("started storage")

	preHooks, err := middleware.NewHooks(cfg.PreHooks, r.storage)
	if err != nil {
		return fmt.Errorf("failed to validate hook config: %w", err)
	}
	postHooks, err := middleware.NewHooks(cfg.PostHooks, r.storage)
	if err != nil {
		return fmt.Errorf("failed to validate hook config: %w", err)
	}

	r.logic = middleware.NewLogic(cfg.AnnounceInterval, cfg.MinAnnounceInterval, r.storage, preHooks, postHooks)

	var started bool
	if len(cfg.HTTPConfig) > 0 {
		log.Info().Object("config", cfg.HTTPConfig).Msg("starting HTTP frontend")
		httpFE, err := http.NewFrontend(r.logic, cfg.HTTPConfig)
		if err == nil {
			r.sg.Add(httpFE)
			started = true
		} else {
			return err
		}
	}

	if len(cfg.UDPConfig) > 0 {
		log.Info().Object("config", cfg.UDPConfig).Msg("starting UDP frontend")
		udpFE, err := udp.NewFrontend(r.logic, cfg.UDPConfig)
		if err == nil {
			r.sg.Add(udpFE)
			started = true
		} else {
			return err
		}
	}
	if !started {
		return errors.New("no frontends configured")
	}

	return nil
}

// Dispose shuts down an instance of Server.
func (r *Server) Dispose() {
	log.Debug().Msg("stopping frontends and metrics server")
	if errs := r.sg.Stop().Wait(); len(errs) > 0 {
		log.Error().Errs("errors", errs).Msg("error occurred while shutting down frontends")
	}

	log.Debug().Msg("stopping logic")
	if errs := r.logic.Stop().Wait(); len(errs) > 0 {
		log.Error().Errs("errors", errs).Msg("error occurred while shutting down middlewares")
	}

	log.Debug().Msg("stopping peer store")
	if errs := r.storage.Stop().Wait(); len(errs) != 0 {
		log.Error().Errs("errors", errs).Msg("error occurred while shutting down peer store")
	}
	log.Close()
}
