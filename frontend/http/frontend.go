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
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/stop"
)

var logger = log.NewLogger("http frontend")

// Config represents all of the configurable options for an HTTP BitTorrent
// Frontend.
type Config struct {
	Addr                string
	HTTPSAddr           string        `cfg:"https_addr"`
	ReadTimeout         time.Duration `cfg:"read_timeout"`
	WriteTimeout        time.Duration `cfg:"write_timeout"`
	IdleTimeout         time.Duration `cfg:"idle_timeout"`
	EnableKeepAlive     bool          `cfg:"enable_keepalive"`
	TLSCertPath         string        `cfg:"tls_cert_path"`
	TLSKeyPath          string        `cfg:"tls_key_path"`
	AnnounceRoutes      []string      `cfg:"announce_routes"`
	ScrapeRoutes        []string      `cfg:"scrape_routes"`
	PingRoutes          []string      `cfg:"ping_routes"`
	EnableRequestTiming bool          `cfg:"enable_request_timing"`
	ParseOptions
}

// Default config constants.
const (
	defaultReadTimeout  = 2 * time.Second
	defaultWriteTimeout = 2 * time.Second
	defaultIdleTimeout  = 30 * time.Second
)

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() Config {
	validcfg := cfg

	if cfg.ReadTimeout <= 0 {
		validcfg.ReadTimeout = defaultReadTimeout
		logger.Warn().
			Str("name", "http.ReadTimeout").
			Dur("provided", cfg.ReadTimeout).
			Dur("default", validcfg.ReadTimeout).
			Msg("falling back to default configuration")
	}

	if cfg.WriteTimeout <= 0 {
		validcfg.WriteTimeout = defaultWriteTimeout
		logger.Warn().
			Str("name", "http.WriteTimeout").
			Dur("provided", cfg.WriteTimeout).
			Dur("default", validcfg.WriteTimeout).
			Msg("falling back to default configuration")
	}

	if cfg.IdleTimeout <= 0 {
		validcfg.IdleTimeout = defaultIdleTimeout

		if cfg.EnableKeepAlive {
			// If keepalive is disabled, this configuration isn't used anyway.
			logger.Warn().
				Str("name", "http.IdleTimeout").
				Dur("provided", cfg.IdleTimeout).
				Dur("default", validcfg.IdleTimeout).
				Msg("falling back to default configuration")
		}
	}

	validcfg.ParseOptions.ParseOptions = cfg.ParseOptions.ParseOptions.Validate()

	return validcfg
}

// Frontend represents the state of an HTTP BitTorrent Frontend.
type Frontend struct {
	srv    *http.Server
	tlsSrv *http.Server
	tlsCfg *tls.Config

	logic frontend.TrackerLogic
	Config
}

// NewFrontend creates a new instance of an HTTP Frontend that asynchronously
// serves requests.
func NewFrontend(logic frontend.TrackerLogic, c conf.MapConfig) (*Frontend, error) {
	var provided Config
	if err := c.Unmarshal(&provided); err != nil {
		return nil, err
	}
	cfg := provided.Validate()

	f := &Frontend{
		logic:  logic,
		Config: cfg,
	}

	if cfg.Addr == "" && cfg.HTTPSAddr == "" {
		return nil, errors.New("must specify addr or https_addr or both")
	}

	if len(cfg.AnnounceRoutes) < 1 || len(cfg.ScrapeRoutes) < 1 {
		return nil, errors.New("must specify routes")
	}

	// If TLS is enabled, create a key pair.
	if cfg.TLSCertPath != "" && cfg.TLSKeyPath != "" {
		var err error
		f.tlsCfg = &tls.Config{
			Certificates: make([]tls.Certificate, 1),
			MinVersion:   tls.VersionTLS12,
		}
		f.tlsCfg.Certificates[0], err = tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
		if err != nil {
			return nil, err
		}
	}

	if (cfg.HTTPSAddr == "") != (f.tlsCfg == nil) {
		return nil, errors.New("must specify both https_addr, tls_cert_path and tls_key_path")
	}

	router := httprouter.New()
	for _, route := range f.AnnounceRoutes {
		router.GET(route, f.announceRoute)
	}
	for _, route := range f.ScrapeRoutes {
		router.GET(route, f.scrapeRoute)
	}

	if len(f.PingRoutes) > 0 {
		for _, route := range f.PingRoutes {
			router.GET(route, f.ping)
			router.HEAD(route, f.ping)
		}
	}

	if cfg.Addr != "" {
		go func() {
			if err := f.serveHTTP(router, false); err != nil {
				logger.Fatal().Err(err).Str("proto", "http").Msg("failed while serving")
			}
		}()
	}

	if cfg.HTTPSAddr != "" {
		go func() {
			if err := f.serveHTTP(router, true); err != nil {
				logger.Fatal().Err(err).Str("proto", "https").Msg("failed while serving")
			}
		}()
	}

	return f, nil
}

// Stop provides a thread-safe way to shut down a currently running Frontend.
func (f *Frontend) Stop() stop.Result {
	stopGroup := stop.NewGroup()

	if f.srv != nil {
		stopGroup.AddFunc(f.makeStopFunc(f.srv))
	}
	if f.tlsSrv != nil {
		stopGroup.AddFunc(f.makeStopFunc(f.tlsSrv))
	}

	return stopGroup.Stop()
}

func (f *Frontend) makeStopFunc(stopSrv *http.Server) stop.Func {
	return func() stop.Result {
		c := make(stop.Channel)
		go func() {
			c.Done(stopSrv.Shutdown(context.Background()))
		}()
		return c.Result()
	}
}

// serveHTTP blocks while listening and serving non-TLS HTTP BitTorrent
// requests until Stop() is called or an error is returned.
func (f *Frontend) serveHTTP(handler http.Handler, tls bool) error {
	srv := &http.Server{
		Handler:      handler,
		ReadTimeout:  f.ReadTimeout,
		WriteTimeout: f.WriteTimeout,
		IdleTimeout:  f.IdleTimeout,
	}

	srv.SetKeepAlivesEnabled(f.EnableKeepAlive)

	var err error
	if tls {
		srv.Addr = f.HTTPSAddr
		srv.TLSConfig = f.tlsCfg
		f.tlsSrv = srv
		err = srv.ListenAndServeTLS("", "")
	} else {
		srv.Addr = f.Addr
		f.srv = srv
		err = srv.ListenAndServe()
	}
	// Start the HTTP server.
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func injectRouteParamsToContext(ctx context.Context, ps httprouter.Params) context.Context {
	rp := bittorrent.RouteParams{}
	for _, p := range ps {
		rp = append(rp, bittorrent.RouteParam{Key: p.Key, Value: p.Value})
	}
	return context.WithValue(ctx, bittorrent.RouteParamsKey, rp)
}

// announceRoute parses and responds to an Announce.
func (f *Frontend) announceRoute(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var err error
	var start time.Time
	var addr netip.Addr
	var req *bittorrent.AnnounceRequest
	if f.EnableRequestTiming && metrics.Enabled() {
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
func (f *Frontend) scrapeRoute(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var err error
	var start time.Time
	var addr netip.Addr
	if f.EnableRequestTiming && metrics.Enabled() {
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

func (f Frontend) ping(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusOK)
}
