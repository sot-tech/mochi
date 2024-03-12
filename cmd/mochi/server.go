package main

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/rs/zerolog"

	"github.com/sot-tech/mochi/frontend"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/storage"
)

// Server represents the state of a running instance.
type Server struct {
	frontends []io.Closer
	hooks     []io.Closer
	storage   storage.PeerStorage
}

// Run begins an instance of Conf.
// It is optional to provide an instance of the peer store to avoid the
// creation of a new one.
func (r *Server) Run(cfg *Config) (err error) {
	if len(cfg.MetricsAddr) > 0 {
		log.Info().Str("addr", cfg.MetricsAddr).Msg("starting metrics server")
		r.frontends = append(r.frontends, metrics.NewServer(cfg.MetricsAddr))
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

	for _, h := range preHooks {
		if c, isOk := h.(io.Closer); isOk {
			r.hooks = append(r.hooks, c)
		}
	}

	postHooks, err := middleware.NewHooks(cfg.PostHooks, r.storage)
	if err != nil {
		return fmt.Errorf("failed to configure post-hooks: %w", err)
	}

	for _, h := range postHooks {
		if c, isOk := h.(io.Closer); isOk {
			r.hooks = append(r.hooks, c)
		}
	}

	if len(cfg.Frontends) > 0 {
		var fs []frontend.Frontend
		logic := middleware.NewLogic(cfg.AnnounceInterval, cfg.MinAnnounceInterval, r.storage, preHooks, postHooks)
		if fs, err = frontend.NewFrontends(cfg.Frontends, logic); err == nil {
			for _, f := range fs {
				r.frontends = append(r.frontends, f)
			}
		} else {
			err = fmt.Errorf("failed to configure frontends: %w", err)
		}
	} else {
		err = errors.New("no frontends configured")
	}

	return err
}

// Shutdown shuts down an instance of Server.
func (r *Server) Shutdown() {
	log.Debug().Msg("stopping frontends and metrics server")
	closeGroup(r.frontends).Msg("frontends stopped")

	log.Debug().Msg("stopping middleware")
	closeGroup(r.hooks).Msg("hooks stopped")

	log.Debug().Msg("stopping peer store")
	var err error
	if r.storage != nil {
		err = r.storage.Close()
	} else {
		err = errors.New("peer store not configured")
	}
	log.Err(err).Msg("peer store stopped")
	log.Close()
}

func closeGroup(cls []io.Closer) (e *zerolog.Event) {
	l := len(cls)
	errs := make([]error, l)
	wg := sync.WaitGroup{}
	wg.Add(l)
	for i, cl := range cls {
		go func(i int, cl io.Closer) {
			defer wg.Done()
			if e := cl.Close(); e != nil {
				errs[i] = e
			}
		}(i, cl)
	}
	wg.Wait()
	nnErrs := make([]error, 0, l)
	for _, e := range errs {
		if e != nil {
			nnErrs = append(nnErrs, e)
		}
	}
	var evt *zerolog.Event
	if len(nnErrs) > 0 {
		evt = log.Error().Errs("errors", nnErrs)
	} else {
		evt = log.Info()
	}
	return evt
}
