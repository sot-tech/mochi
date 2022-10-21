package main

import (
	"errors"
	"fmt"

	"github.com/sot-tech/mochi/frontend"
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
	cfg, err := ParseConfigFile(configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	r.sg = stop.NewGroup()

	if len(cfg.MetricsAddr) > 0 {
		log.Info().Str("address", cfg.MetricsAddr).Msg("starting metrics server")
		r.sg.Add(metrics.NewServer(cfg.MetricsAddr))
	} else {
		log.Info().Msg("metrics disabled because of empty address")
	}

	r.storage, err = storage.NewStorage(cfg.Storage)
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	preHooks, err := middleware.NewHooks(cfg.PreHooks, r.storage)
	if err != nil {
		return fmt.Errorf("failed to configure pre-hooks: %w", err)
	}
	postHooks, err := middleware.NewHooks(cfg.PostHooks, r.storage)
	if err != nil {
		return fmt.Errorf("failed to configure post-hooks: %w", err)
	}

	if len(cfg.Frontends) > 0 {
		var fs []frontend.Frontend
		r.logic = middleware.NewLogic(cfg.AnnounceInterval, cfg.MinAnnounceInterval, r.storage, preHooks, postHooks)
		if fs, err = frontend.NewFrontends(cfg.Frontends, r.logic); err == nil {
			for _, f := range fs {
				r.sg.Add(f)
			}
		} else {
			err = fmt.Errorf("failed to configure frontends: %w", err)
		}
	} else {
		err = errors.New("no frontends configured")
	}

	return err
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
