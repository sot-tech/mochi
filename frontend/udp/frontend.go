// Package udp implements a BitTorrent tracker via the UDP protocol as
// described in BEP 15.
package udp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/frontend"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/bytepool"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/timecache"
)

var (
	logger                          = log.NewLogger("frontend/udp")
	allowedGeneratedPrivateKeyRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
)

func init() {
	frontend.RegisterBuilder("udp", NewFrontend)
}

// Config represents all the configurable options for a UDP BitTorrent
// Tracker.
type Config struct {
	frontend.ListenOptions
	PrivateKey   string        `cfg:"private_key"`
	MaxClockSkew time.Duration `cfg:"max_clock_skew"`
	frontend.ParseOptions
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
func (cfg Config) Validate() (validCfg Config, err error) {
	if len(cfg.Addr) == 0 {
		err = frontend.ErrAddressNotProvided
		return
	}

	validCfg = cfg

	// Generate a private key if one isn't provided by the user.
	if cfg.PrivateKey == "" {
		pkeyRunes := make([]rune, 64)
		for i := range pkeyRunes {
			pkeyRunes[i] = allowedGeneratedPrivateKeyRunes[rand.Intn(len(allowedGeneratedPrivateKeyRunes))]
		}
		validCfg.PrivateKey = string(pkeyRunes)

		logger.Warn().
			Str("name", "PrivateKey").
			Str("provided", "").
			Str("key", validCfg.PrivateKey).
			Msg("falling back to default configuration")
	}

	validCfg.ParseOptions = cfg.ParseOptions.Validate()

	return
}

// udpFE holds the state of a UDP BitTorrent Frontend.
type udpFE struct {
	socket         *net.UDPConn
	closing        chan any
	wg             sync.WaitGroup
	genPool        *sync.Pool
	logic          *middleware.Logic
	maxClockSkew   time.Duration
	collectTimings bool
	ctxCancel      context.CancelFunc
	onceCloser     sync.Once
	frontend.ParseOptions
}

// NewFrontend builds and starts udp bittorrent frontend from provided configuration
func NewFrontend(c conf.MapConfig, logic *middleware.Logic) (frontend.Frontend, error) {
	var err error
	var cfg Config
	if err = c.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	if cfg, err = cfg.Validate(); err != nil {
		return nil, err
	}

	f := &udpFE{
		closing:        make(chan any),
		logic:          logic,
		maxClockSkew:   cfg.MaxClockSkew,
		collectTimings: cfg.EnableRequestTiming,
		ParseOptions:   cfg.ParseOptions,
		genPool: &sync.Pool{
			New: func() any {
				return NewConnectionIDGenerator(cfg.PrivateKey)
			},
		},
	}

	if f.socket, err = cfg.ListenUDP(); err == nil {
		var ctx context.Context
		ctx, f.ctxCancel = context.WithCancel(context.Background())
		f.wg.Add(1)
		go func(ctx context.Context) {
			if err := f.serve(ctx); err != nil {
				logger.Fatal().Err(err).Msg("server failed")
			}
		}(ctx)
	}

	return f, err
}

// Close provides a thread-safe way to shut down a currently running Frontend.
func (t *udpFE) Close() (err error) {
	t.onceCloser.Do(func() {
		close(t.closing)
		if t.socket != nil {
			t.ctxCancel()
			_ = t.socket.SetReadDeadline(time.Now())
			t.wg.Wait()
			err = t.socket.Close()
		}
	})

	return
}

// serve blocks while listening and serving UDP BitTorrent requests
// until Stop() is called or an error is returned.
func (t *udpFE) serve(ctx context.Context) error {
	pool := bytepool.NewBytePool(2048)
	defer t.wg.Done()

	for {
		// Check to see if we need shutdown.
		select {
		case <-t.closing:
			log.Debug().Msg("serve received shutdown signal")
			return nil
		default:
		}

		// Read a UDP packet into a reusable buffer.
		buffer := pool.Get()
		n, addrPort, err := t.socket.ReadFromUDPAddrPort(*buffer)
		if err != nil {
			pool.Put(buffer)
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// A temporary failure is not fatal; just pretend it never happened.
				continue
			}
			return err
		}

		// We got nothin'
		if n == 0 {
			pool.Put(buffer)
			continue
		}

		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer pool.Put(buffer)

			// Handle the request.
			addr := addrPort.Addr().Unmap()
			var start time.Time
			if t.collectTimings && metrics.Enabled() {
				start = time.Now()
			}
			action, err := t.handleRequest(ctx,
				Request{(*buffer)[:n], addr},
				ResponseWriter{t.socket, addrPort},
			)
			if t.collectTimings && metrics.Enabled() {
				recordResponseDuration(action, addr, err, time.Since(start))
			}
		}()
	}
}

// Request represents a UDP payload received by a Tracker.
type Request struct {
	Packet []byte
	IP     netip.Addr
}

// ResponseWriter implements the ability to respond to a Request via the
// io.Writer interface.
type ResponseWriter struct {
	socket   *net.UDPConn
	addrPort netip.AddrPort
}

// Write implements the io.Writer interface for a ResponseWriter.
func (w ResponseWriter) Write(b []byte) (int, error) {
	return w.socket.WriteToUDPAddrPort(b, w.addrPort)
}

// handleRequest parses and responds to a UDP Request.
func (t *udpFE) handleRequest(ctx context.Context, r Request, w ResponseWriter) (actionName string, err error) {
	if len(r.Packet) < 16 {
		// Malformed, no client packets are less than 16 bytes.
		// We explicitly return nothing in case this is a DoS attempt.
		err = errMalformedPacket
		return
	}

	// Parse the headers of the UDP packet.
	connID := r.Packet[0:8]
	actionID := binary.BigEndian.Uint32(r.Packet[8:12])
	txID := r.Packet[12:16]

	// get a connection ID generator/validator from the pool.
	gen := t.genPool.Get().(*ConnectionIDGenerator)
	defer t.genPool.Put(gen)

	// If this isn't requesting a new connection ID and the connection ID is
	// invalid, then fail.
	if actionID != connectActionID && !gen.Validate(connID, r.IP, timecache.Now(), t.maxClockSkew) {
		err = errBadConnectionID
		WriteError(w, txID, err)
		return
	}

	// Handle the requested action.
	switch actionID {
	case connectActionID:
		actionName = "connect"

		if !bytes.Equal(connID, initialConnectionID) {
			err = errMalformedPacket
			return
		}

		WriteConnectionID(w, txID, gen.Generate(r.IP, timecache.Now()))

	case announceActionID, announceV6ActionID:
		actionName = "announce"

		var req *bittorrent.AnnounceRequest
		req, err = ParseAnnounce(r, actionID == announceV6ActionID, t.ParseOptions)
		if err != nil {
			WriteError(w, txID, err)
			return
		}

		var resp *bittorrent.AnnounceResponse
		ctx := bittorrent.InjectRouteParamsToContext(ctx, bittorrent.RouteParams{})
		ctx, resp, err = t.logic.HandleAnnounce(ctx, req)
		if err != nil {
			WriteError(w, txID, err)
			return
		}

		WriteAnnounce(w, txID, resp, actionID == announceV6ActionID, r.IP.Is6())

		ctx = bittorrent.RemapRouteParamsToBgContext(ctx)
		go t.logic.AfterAnnounce(ctx, req, resp)

	case scrapeActionID:
		actionName = "scrape"

		var req *bittorrent.ScrapeRequest
		req, err = ParseScrape(r, t.ParseOptions)
		if err != nil {
			WriteError(w, txID, err)
			return
		}

		var resp *bittorrent.ScrapeResponse
		ctx := bittorrent.InjectRouteParamsToContext(ctx, bittorrent.RouteParams{})
		ctx, resp, err = t.logic.HandleScrape(ctx, req)
		if err != nil {
			WriteError(w, txID, err)
			return
		}

		WriteScrape(w, txID, resp)

		ctx = bittorrent.RemapRouteParamsToBgContext(ctx)
		go t.logic.AfterScrape(ctx, req, resp)

	default:
		err = errUnknownAction
		WriteError(w, txID, err)
	}

	return
}
