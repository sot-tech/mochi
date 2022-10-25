// Package http implements a BitTorrent frontend via the HTTP protocol as
// described in BEP 3 and BEP 23.
package http

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/netip"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/frontend"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/stop"
)

var (
	logger               = log.NewLogger("frontend/http")
	errTLSNotProvided    = errors.New("tls certificate/key not provided")
	errRoutesNotProvided = errors.New("routes not provided")
)

func init() {
	frontend.RegisterBuilder("http", NewFrontend)
}

// Config represents all configurable options for an HTTP BitTorrent Frontend
type Config struct {
	frontend.ListenOptions
	IdleTimeout     time.Duration `cfg:"idle_timeout"`
	EnableKeepAlive bool          `cfg:"enable_keepalive"`
	UseTLS          bool          `cfg:"tls"`
	TLSCertPath     string        `cfg:"tls_cert_path"`
	TLSKeyPath      string        `cfg:"tls_key_path"`
	AnnounceRoutes  []string      `cfg:"announce_routes"`
	ScrapeRoutes    []string      `cfg:"scrape_routes"`
	PingRoutes      []string      `cfg:"ping_routes"`
	ParseOptions
}

const defaultIdleTimeout = 30 * time.Second

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
func (cfg Config) Validate() (validCfg Config, err error) {
	validCfg = cfg
	if validCfg.ListenOptions, err = cfg.ListenOptions.Validate(); err != nil {
		return
	}
	if cfg.UseTLS && (len(cfg.TLSCertPath) == 0 || len(cfg.TLSKeyPath) == 0) {
		err = errTLSNotProvided
		return
	}
	if cfg.IdleTimeout <= 0 {
		validCfg.IdleTimeout = defaultIdleTimeout
		if cfg.EnableKeepAlive {
			// If keepalive is disabled, this configuration isn't used anyway.
			logger.Warn().
				Str("name", "IdleTimeout").
				Dur("provided", cfg.IdleTimeout).
				Dur("default", validCfg.IdleTimeout).
				Msg("falling back to default configuration")
		}
	}
	validCfg.ParseOptions.ParseOptions = cfg.ParseOptions.ParseOptions.Validate()
	return
}

type httpFE struct {
	srv            *http.Server
	logic          *middleware.Logic
	collectTimings bool
	ParseOptions
}

// NewFrontend builds and starts http bittorrent frontend from provided configuration
func NewFrontend(c conf.MapConfig, logic *middleware.Logic) (_ frontend.Frontend, err error) {
	var cfg Config
	if err = c.Unmarshal(&cfg); err != nil {
		return
	}
	if cfg, err = cfg.Validate(); err != nil {
		return
	}
	if len(cfg.AnnounceRoutes) < 1 || len(cfg.ScrapeRoutes) < 1 {
		err = errRoutesNotProvided
		return
	}

	f := &httpFE{
		logic: logic,
		srv: &http.Server{
			ReadTimeout:       cfg.ReadTimeout,
			ReadHeaderTimeout: cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
		collectTimings: cfg.EnableRequestTiming,
		ParseOptions:   cfg.ParseOptions,
	}

	// If TLS is enabled, create a key pair.
	if cfg.UseTLS {
		var cert tls.Certificate
		if cert, err = tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
			return
		}
		f.srv.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	router := httprouter.New()
	for _, route := range cfg.AnnounceRoutes {
		router.GET(route, f.announceRoute)
	}
	for _, route := range cfg.ScrapeRoutes {
		router.GET(route, f.scrapeRoute)
	}
	for _, route := range cfg.PingRoutes {
		router.GET(route, f.ping)
		router.HEAD(route, f.ping)
	}
	f.srv.Handler = router

	f.srv.SetKeepAlivesEnabled(cfg.EnableKeepAlive)

	go func() {
		ln, err := cfg.ListenTCP()
		defer func() {
			logger.Err(ln.Close()).Msg("closing listener")
		}()
		if err == nil {
			if f.srv.TLSConfig == nil {
				err = f.srv.Serve(ln)
			} else {
				err = f.srv.ServeTLS(ln, "", "")
			}
		}
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Err(err).Msg("server failed")
		}
	}()

	return f, nil
}

// Stop provides a thread-safe way to shut down a currently running Frontend.
func (f *httpFE) Stop() stop.Result {
	c := make(stop.Channel)
	if f.srv != nil {
		go func() {
			c.Done(f.srv.Shutdown(context.Background()))
		}()
	}
	return c.Result()
}

func injectRouteParamsToContext(ctx context.Context, ps httprouter.Params) context.Context {
	rp := bittorrent.RouteParams{}
	for _, p := range ps {
		rp = append(rp, bittorrent.RouteParam{Key: p.Key, Value: p.Value})
	}
	return context.WithValue(ctx, bittorrent.RouteParamsKey, rp)
}

// announceRoute parses and responds to an Announce.
func (f *httpFE) announceRoute(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var err error
	var start time.Time
	var addr netip.Addr
	var req *bittorrent.AnnounceRequest
	if f.collectTimings && metrics.Enabled() {
		start = time.Now()
		defer func() {
			recordResponseDuration("announce", addr, err, time.Since(start))
		}()
	}

	req, err = ParseAnnounce(r, f.ParseOptions)
	if err != nil {
		WriteError(w, err)
		return
	}
	addr = req.GetFirst()

	ctx := injectRouteParamsToContext(context.Background(), ps)
	ctx, resp, err := f.logic.HandleAnnounce(ctx, req)
	if err != nil {
		WriteError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	err = WriteAnnounceResponse(w, resp)
	if err != nil {
		WriteError(w, err)
		return
	}

	go f.logic.AfterAnnounce(ctx, req, resp)
}

// scrapeRoute parses and responds to a Scrape.
func (f *httpFE) scrapeRoute(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var err error
	var start time.Time
	var addr netip.Addr
	if f.collectTimings && metrics.Enabled() {
		start = time.Now()
		defer func() {
			recordResponseDuration("scrape", addr, err, time.Since(start))
		}()
	}

	req, err := ParseScrape(r, f.ParseOptions)
	if err != nil {
		WriteError(w, err)
		return
	}
	addr = req.GetFirst()

	ctx := injectRouteParamsToContext(context.Background(), ps)
	ctx, resp, err := f.logic.HandleScrape(ctx, req)
	if err != nil {
		WriteError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	err = WriteScrapeResponse(w, resp)
	if err != nil {
		WriteError(w, err)
		return
	}

	go f.logic.AfterScrape(ctx, req, resp)
}

func (f *httpFE) ping(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var err error
	if r.Method == http.MethodGet {
		err = f.logic.Ping(context.TODO())
	}
	if err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		logger.Error().Err(err).Msg("ping completed with error")
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}
