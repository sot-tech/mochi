// Package metrics implements a standalone HTTP server for serving pprof
// profiles and Prometheus metrics.
package metrics

import (
	"errors"
	"net/http"
	"net/http/pprof"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/fasthttp/router"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"

	"github.com/sot-tech/mochi/pkg/log"
)

const (
	readTimeout  = time.Minute
	writeTimeout = readTimeout * 2
)

var (
	logger        = log.NewLogger("metrics")
	serverCounter = new(int32)
)

// Enabled indicates that configured at least one metrics server
func Enabled() bool {
	return atomic.LoadInt32(serverCounter) > 0
}

// Server represents a standalone HTTP server for serving a Prometheus metrics
// endpoint.
type Server struct {
	srv *fasthttp.Server
}

// AddressFamily returns the label value for reporting the address family of an IP address.
func AddressFamily(ip netip.Addr) string {
	switch {
	case ip.Is4(), ip.Is4In6():
		return "IPv4"
	case ip.Is6():
		return "IPv6"
	default:
		return "<unknown>"
	}
}

// Close shuts down the server.
func (s *Server) Close() error {
	return s.srv.Shutdown()
}

// NewServer creates a new instance of a Prometheus server that asynchronously
// serves requests.
func NewServer(addr string) *Server {
	if len(addr) == 0 {
		panic("metrics listen address not provided")
	}

	r := router.New()
	r.GET("/metrics", fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler()))
	r.GET("/debug/pprof/", fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Index))
	r.GET("/debug/pprof/cmdline", fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Cmdline))
	r.GET("/debug/pprof/profile", fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Profile))
	r.GET("/debug/pprof/symbol", fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Symbol))
	r.GET("/debug/pprof/trace", fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Trace))

	s := &Server{
		srv: &fasthttp.Server{
			Handler:      r.Handler,
			GetOnly:      true,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
		},
	}

	go func() {
		atomic.AddInt32(serverCounter, 1)
		defer atomic.AddInt32(serverCounter, -1)
		if err := s.srv.ListenAndServe(addr); !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("failed while serving prometheus")
		}
	}()

	return s
}
