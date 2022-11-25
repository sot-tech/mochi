// Package http implements a BitTorrent frontend via the HTTP protocol as
// described in BEP 3 and BEP 23.
package http

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/frontend"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
)

// Name - registered name of the frontend
const Name = "http"

var (
	logger            = log.NewLogger("frontend/http")
	errTLSNotProvided = errors.New("tls certificate/key not provided")
)

func init() {
	frontend.RegisterBuilder(Name, NewFrontend)
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

const (
	defaultIdleTimeout   = 30 * time.Second
	defaultAnnounceRoute = "/announce"
	defaultScrapeRoute   = "/scrape"
)

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
func (cfg Config) Validate() (validCfg Config, err error) {
	validCfg = cfg
	validCfg.ListenOptions = cfg.ListenOptions.Validate(false, logger)
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
	if len(cfg.AnnounceRoutes) == 0 {
		validCfg.AnnounceRoutes = []string{defaultAnnounceRoute}
		logger.Warn().
			Str("name", "AnnounceRoutes").
			Strs("provided", cfg.AnnounceRoutes).
			Strs("default", validCfg.AnnounceRoutes).
			Msg("falling back to default configuration")
	}
	if len(cfg.ScrapeRoutes) == 0 {
		validCfg.ScrapeRoutes = []string{defaultScrapeRoute}
		logger.Warn().
			Str("name", "ScrapeRoutes").
			Strs("provided", cfg.ScrapeRoutes).
			Strs("default", validCfg.ScrapeRoutes).
			Msg("falling back to default configuration")
	}
	validCfg.ParseOptions.ParseOptions = cfg.ParseOptions.ParseOptions.Validate(logger)
	return
}

// httpServer replaces http.Close method with http.Shutdown
type httpServer struct {
	*http.Server
}

func (c httpServer) Close() (err error) {
	if c.Server != nil {
		err = c.Shutdown(context.Background())
	}
	return
}

type httpFE struct {
	servers        []httpServer
	logic          *middleware.Logic
	collectTimings bool
	onceCloser     sync.Once

	ParseOptions
}

// NewFrontend builds and starts http bittorrent frontend from provided configuration
func NewFrontend(c conf.MapConfig, logic *middleware.Logic) (frontend.Frontend, error) {
	var cfg Config
	var err error
	if err = c.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	if cfg, err = cfg.Validate(); err != nil {
		return nil, err
	}

	f := &httpFE{
		logic:          logic,
		servers:        make([]httpServer, cfg.Workers),
		collectTimings: cfg.EnableRequestTiming,
		ParseOptions:   cfg.ParseOptions,
	}

	for i := range f.servers {
		f.servers[i] = httpServer{
			&http.Server{
				ReadTimeout:       cfg.ReadTimeout,
				ReadHeaderTimeout: cfg.ReadTimeout,
				WriteTimeout:      cfg.WriteTimeout,
				IdleTimeout:       cfg.IdleTimeout,
			},
		}
	}

	// If TLS is enabled, create a key pair.
	if cfg.UseTLS {
		var cert tls.Certificate
		if cert, err = tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
			return nil, err
		}
		certs := []tls.Certificate{cert}
		for i := range f.servers {
			f.servers[i].TLSConfig = &tls.Config{
				Certificates: certs,
				MinVersion:   tls.VersionTLS12,
			}
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

	for _, srv := range f.servers {
		srv.Handler = router
		srv.SetKeepAlivesEnabled(cfg.EnableKeepAlive)
		go runServer(srv, &cfg)
	}

	return f, nil
}

func runServer(s httpServer, cfg *Config) {
	ln, err := cfg.ListenTCP()
	if err == nil {
		if s.TLSConfig == nil {
			err = s.Serve(ln)
		} else {
			err = s.ServeTLS(ln, "", "")
		}
	}
	if !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Err(err).Msg("server failed")
	}
}

// Close provides a thread-safe way to gracefully shut down a currently running Frontend.
func (f *httpFE) Close() (err error) {
	f.onceCloser.Do(func() {
		cls := make([]io.Closer, len(f.servers))
		for i, s := range f.servers {
			cls[i] = s
		}
		err = frontend.CloseGroup(cls)
	})

	return
}

func httpParamsToRouteParams(in httprouter.Params) (out bittorrent.RouteParams) {
	out = make([]bittorrent.RouteParam, 0, len(in))
	for _, p := range in {
		out = append(out, bittorrent.RouteParam{Key: p.Key, Value: p.Value})
	}
	return
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

	ctx := bittorrent.InjectRouteParamsToContext(r.Context(), httpParamsToRouteParams(ps))
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

	// next actions are background and should not be canceled after http writer closed
	ctx = bittorrent.RemapRouteParamsToBgContext(ctx)
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

	ctx := bittorrent.InjectRouteParamsToContext(r.Context(), httpParamsToRouteParams(ps))
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

	// next actions are background and should not be canceled after http writer closed
	ctx = bittorrent.RemapRouteParamsToBgContext(ctx)
	go f.logic.AfterScrape(ctx, req, resp)
}

func (f *httpFE) ping(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var err error
	status := http.StatusOK
	ctx := r.Context()
	if r.Method == http.MethodGet {
		err = f.logic.Ping(ctx)
	}

	if err != nil {
		logger.Error().Err(err).Msg("ping completed with error")
		status = http.StatusServiceUnavailable
	}
	if ctxErr := ctx.Err(); ctxErr == nil {
		w.WriteHeader(status)
	} else {
		logger.Info().Err(ctxErr).Str("ip", r.RemoteAddr).Msg("ping request cancelled")
	}
}
