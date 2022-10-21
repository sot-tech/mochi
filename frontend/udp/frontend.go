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

	"github.com/libp2p/go-reuseport"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/frontend"
	"github.com/sot-tech/mochi/frontend/udp/bytepool"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/pkg/timecache"
)

const Name = "http"

var (
	logger                          = log.NewLogger(Name)
	allowedGeneratedPrivateKeyRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	errUnexpectedConnType           = errors.New("unexpected connection type (not UDPConn)")
)

func init() {
	frontend.RegisterBuilder(Name, newFrontend)
}

// Config represents all of the configurable options for a UDP BitTorrent
// Tracker.
type Config struct {
	Addr                string
	ReusePort           bool          `cfg:"reuse_port"`
	PrivateKey          string        `cfg:"private_key"`
	MaxClockSkew        time.Duration `cfg:"max_clock_skew"`
	EnableRequestTiming bool          `cfg:"enable_request_timing"`
	frontend.ParseOptions
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() Config {
	validcfg := cfg

	// Generate a private key if one isn't provided by the user.
	if cfg.PrivateKey == "" {
		pkeyRunes := make([]rune, 64)
		for i := range pkeyRunes {
			pkeyRunes[i] = allowedGeneratedPrivateKeyRunes[rand.Intn(len(allowedGeneratedPrivateKeyRunes))]
		}
		validcfg.PrivateKey = string(pkeyRunes)

		logger.Warn().
			Str("name", "UDP.PrivateKey").
			Str("provided", "").
			Str("key", validcfg.PrivateKey).
			Msg("falling back to default configuration")
	}

	validcfg.ParseOptions = cfg.ParseOptions.Validate()

	return validcfg
}

// udpFE holds the state of a UDP BitTorrent Frontend.
type udpFE struct {
	socket  *net.UDPConn
	closing chan struct{}
	wg      sync.WaitGroup

	genPool *sync.Pool

	logic *middleware.Logic
	Config
}

func newFrontend(c conf.MapConfig, logic *middleware.Logic) (frontend.Frontend, error) {
	var provided Config
	if err := c.Unmarshal(&provided); err != nil {
		return nil, err
	}
	cfg := provided.Validate()

	f := &udpFE{
		closing: make(chan struct{}),
		logic:   logic,
		Config:  cfg,
		genPool: &sync.Pool{
			New: func() any {
				return NewConnectionIDGenerator(cfg.PrivateKey)
			},
		},
	}

	if err := f.listen(); err != nil {
		return nil, err
	}

	f.wg.Add(1)
	go func() {
		if err := f.serve(); err != nil {
			logger.Fatal().Err(err).Str("proto", "udp").Msg("failed while serving")
		}
	}()

	return f, nil
}

// Stop provides a thread-safe way to shut down a currently running Frontend.
func (t *udpFE) Stop() stop.Result {
	select {
	case <-t.closing:
		return stop.AlreadyStopped
	default:
	}

	c := make(stop.Channel)
	go func() {
		close(t.closing)
		_ = t.socket.SetReadDeadline(time.Now())
		t.wg.Wait()
		c.Done(t.socket.Close())
	}()

	return c.Result()
}

// listen resolves the address and binds the server socket.
func (t *udpFE) listen() (err error) {
	if t.ReusePort {
		var ln net.PacketConn
		if ln, err = reuseport.ListenPacket("udp", t.Addr); err == nil {
			var isOk bool
			if t.socket, isOk = ln.(*net.UDPConn); !isOk {
				err = errUnexpectedConnType
			}
		}
	} else {
		var udpAddr *net.UDPAddr
		udpAddr, err = net.ResolveUDPAddr("udp", t.Addr)
		if err == nil {
			t.socket, err = net.ListenUDP("udp", udpAddr)
		}
	}
	return err
}

// serve blocks while listening and serving UDP BitTorrent requests
// until Stop() is called or an error is returned.
func (t *udpFE) serve() error {
	pool := bytepool.New(2048)
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
			if t.EnableRequestTiming && metrics.Enabled() {
				start = time.Now()
			}
			action, err := t.handleRequest(
				Request{(*buffer)[:n], addr},
				ResponseWriter{t.socket, addrPort},
			)
			if t.EnableRequestTiming && metrics.Enabled() {
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
func (t *udpFE) handleRequest(r Request, w ResponseWriter) (actionName string, err error) {
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
	if actionID != connectActionID && !gen.Validate(connID, r.IP, timecache.Now(), t.MaxClockSkew) {
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

		var ctx context.Context
		var resp *bittorrent.AnnounceResponse
		ctx, resp, err = t.logic.HandleAnnounce(context.Background(), req)
		if err != nil {
			WriteError(w, txID, err)
			return
		}

		WriteAnnounce(w, txID, resp, actionID == announceV6ActionID, r.IP.Is6())

		go t.logic.AfterAnnounce(ctx, req, resp)

	case scrapeActionID:
		actionName = "scrape"

		var req *bittorrent.ScrapeRequest
		req, err = ParseScrape(r, t.ParseOptions)
		if err != nil {
			WriteError(w, txID, err)
			return
		}

		var ctx context.Context
		var resp *bittorrent.ScrapeResponse
		ctx, resp, err = t.logic.HandleScrape(context.Background(), req)
		if err != nil {
			WriteError(w, txID, err)
			return
		}

		WriteScrape(w, txID, resp)

		go t.logic.AfterScrape(ctx, req, resp)

	default:
		err = errUnknownAction
		WriteError(w, txID, err)
	}

	return
}
