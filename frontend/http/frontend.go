// Package http implements a BitTorrent frontend via the HTTP protocol as
// described in BEP 3 and BEP 23.
package http

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/netip"
	"path"
	"sync"
	"time"

	"github.com/valyala/fasthttp"

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
	ReadTimeout     time.Duration `cfg:"read_timeout"`
	WriteTimeout    time.Duration `cfg:"write_timeout"`
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
	defaultReadTimeout  = 2 * time.Second
	defaultWriteTimeout = 2 * time.Second
	defaultIdleTimeout  = 30 * time.Second
	// DefaultAnnounceRoute is the default url path to listen announce
	// requests if nothing else provided
	DefaultAnnounceRoute = "/announce"
	// DefaultScrapeRoute is the default url path to listen scrape
	// requests if nothing else provided
	DefaultScrapeRoute = "/scrape"
)

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
func (cfg Config) Validate() (validCfg Config, err error) {
	validCfg = cfg
	validCfg.ListenOptions = cfg.ListenOptions.Validate(logger)
	if cfg.UseTLS && (len(cfg.TLSCertPath) == 0 || len(cfg.TLSKeyPath) == 0) {
		err = errTLSNotProvided
		return validCfg, err
	}

	if cfg.ReadTimeout <= 0 {
		validCfg.ReadTimeout = defaultReadTimeout
		logger.Warn().
			Str("name", "ReadTimeout").
			Dur("provided", cfg.ReadTimeout).
			Dur("default", validCfg.ReadTimeout).
			Msg("falling back to default configuration")
	}

	if cfg.WriteTimeout <= 0 {
		validCfg.WriteTimeout = defaultWriteTimeout
		logger.Warn().
			Str("name", "WriteTimeout").
			Dur("provided", cfg.WriteTimeout).
			Dur("default", validCfg.WriteTimeout).
			Msg("falling back to default configuration")
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
		validCfg.AnnounceRoutes = []string{DefaultAnnounceRoute}
		logger.Warn().
			Str("name", "AnnounceRoutes").
			Strs("provided", cfg.AnnounceRoutes).
			Strs("default", validCfg.AnnounceRoutes).
			Msg("falling back to default configuration")
	}
	if len(cfg.ScrapeRoutes) == 0 {
		validCfg.ScrapeRoutes = []string{DefaultScrapeRoute}
		logger.Warn().
			Str("name", "ScrapeRoutes").
			Strs("provided", cfg.ScrapeRoutes).
			Strs("default", validCfg.ScrapeRoutes).
			Msg("falling back to default configuration")
	}
	validCfg.ParseOptions.ParseOptions = cfg.ParseOptions.Validate(logger)
	return validCfg, err
}

type httpFE struct {
	*fasthttp.Server
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
		collectTimings: cfg.EnableRequestTiming,
		ParseOptions:   cfg.ParseOptions,
		Server: &fasthttp.Server{
			ReadTimeout:      cfg.ReadTimeout,
			WriteTimeout:     cfg.WriteTimeout,
			IdleTimeout:      cfg.IdleTimeout,
			Concurrency:      int(cfg.Workers),
			DisableKeepalive: !cfg.EnableKeepAlive,
			GetOnly:          true,
			Logger:           logger,
		},
	}

	// If TLS is enabled, create a key pair.
	if cfg.UseTLS {
		var cert tls.Certificate
		if cert, err = tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
			return nil, err
		}
		certs := []tls.Certificate{cert}
		f.TLSConfig = &tls.Config{
			Certificates: certs,
			MinVersion:   tls.VersionTLS12,
		}
	}

	pathRouting := make(map[string]func(*fasthttp.RequestCtx),
		len(cfg.AnnounceRoutes)+len(cfg.ScrapeRoutes)+len(cfg.PingRoutes))

	for _, route := range cfg.AnnounceRoutes {
		route = path.Clean(route)
		if !path.IsAbs(route) {
			route = "/" + route
		}
		pathRouting[route] = f.announceRoute
	}
	for _, route := range cfg.ScrapeRoutes {
		route = path.Clean(route)
		if !path.IsAbs(route) {
			route = "/" + route
		}
		pathRouting[path.Clean(route)] = f.scrapeRoute
	}
	for _, route := range cfg.PingRoutes {
		route = path.Clean(route)
		if !path.IsAbs(route) {
			route = "/" + route
		}
		pathRouting[path.Clean(route)] = f.ping
	}

	f.Handler = func(ctx *fasthttp.RequestCtx) {
		if route, exists := pathRouting[string(ctx.Path())]; exists {
			route(ctx)
		} else {
			ctx.NotFound()
		}
	}
	go runServer(f.Server, &cfg)

	return f, nil
}

func runServer(s *fasthttp.Server, cfg *Config) {
	logger.Debug().Str("addr", cfg.Addr).Msg("starting listener")
	ln, err := cfg.ListenTCP()
	if err == nil {
		if s.TLSConfig == nil {
			err = s.Serve(ln)
		} else {
			err = s.ServeTLS(ln, "", "")
		}
	}
	defer ln.Close()
	if err == nil {
		logger.Info().Str("addr", cfg.Addr).Msg("listener stopped")
	} else if !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Str("addr", cfg.Addr).Err(err).Msg("listener failed")
	}
}

// Close provides a thread-safe way to gracefully shut down a currently running Frontend.
func (f *httpFE) Close() (err error) {
	f.onceCloser.Do(func() {
		if f.Server != nil {
			err = f.Shutdown()
		}
	})

	return err
}

// announceRoute parses and responds to an Announce.
func (f *httpFE) announceRoute(reqCtx *fasthttp.RequestCtx) {
	var err error
	var start time.Time
	var addr netip.Addr
	var aReq *bittorrent.AnnounceRequest
	if f.collectTimings && metrics.Enabled() {
		start = time.Now()
		defer func() {
			recordResponseDuration("announce", addr, err, time.Since(start))
		}()
	}

	aReq, err = parseAnnounce(reqCtx, f.ParseOptions)
	if err != nil {
		writeErrorResponse(reqCtx, err)
		return
	}
	addr = aReq.GetFirst()

	ctx := bittorrent.InjectRouteParamsToContext(reqCtx, nil)
	ctx, aResp, err := f.logic.HandleAnnounce(ctx, aReq)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			writeErrorResponse(reqCtx, err)
		}
		return
	}

	if err = reqCtx.Err(); err == nil {
		reqCtx.Response.Header.Set("Content-Type", "text/plain; charset=utf-8")
		qArgs := reqCtx.QueryArgs()
		// `compact` means that tracker should return addresses in
		// binary (single concatenated string) mode instead of dictionary.
		// `no_peer_id` means, that tracker may omit PeerID field in response dictionary.
		// see https://wiki.theory.org/BitTorrentSpecification#Tracker_Request_Parameters
		writeAnnounceResponse(reqCtx, aResp, qArgs.GetBool("compact"), !qArgs.GetBool("no_peer_id"))

		// next actions are background and should not be canceled after http writer closed
		ctx = bittorrent.RemapRouteParamsToBgContext(ctx)
		// params mapped from fasthttp.QueryArgs will be reused in the next request
		aReq.Params = nil
		go f.logic.AfterAnnounce(ctx, aReq, aResp)
	}
}

// scrapeRoute parses and responds to a Scrape.
func (f *httpFE) scrapeRoute(reqCtx *fasthttp.RequestCtx) {
	var err error
	var start time.Time
	var addr netip.Addr
	if f.collectTimings && metrics.Enabled() {
		start = time.Now()
		defer func() {
			recordResponseDuration("scrape", addr, err, time.Since(start))
		}()
	}

	req, err := parseScrape(reqCtx, f.ParseOptions)
	if err != nil {
		writeErrorResponse(reqCtx, err)
		return
	}
	addr = req.GetFirst()

	ctx := bittorrent.InjectRouteParamsToContext(reqCtx, nil)
	ctx, resp, err := f.logic.HandleScrape(ctx, req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			writeErrorResponse(reqCtx, err)
		}
		return
	}

	if err = reqCtx.Err(); err == nil {
		reqCtx.SetContentType("text/plain; charset=utf-8")
		writeScrapeResponse(reqCtx, resp)

		// next actions are background and should not be canceled after http writer closed
		ctx = bittorrent.RemapRouteParamsToBgContext(ctx)
		// params mapped from fasthttp.QueryArgs will in the next request
		req.Params = nil
		go f.logic.AfterScrape(ctx, req, resp)
	}
}

func (f *httpFE) ping(ctx *fasthttp.RequestCtx) {
	status := http.StatusOK
	err := f.logic.Ping(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		logger.Error().Err(err).Msg("ping completed with error")
		status = http.StatusServiceUnavailable
	}
	if err = ctx.Err(); err == nil {
		ctx.SetStatusCode(status)
	}
}
