package frontend

import (
	"errors"
	"net"

	"github.com/sot-tech/mochi/pkg/log"

	"github.com/libp2p/go-reuseport"
)

// DefaultListenAddress is the default net listen address
// if nothing else provided
const DefaultListenAddress = ":6969"

var errUnexpectedListenerType = errors.New("unexpected listener type")

// ListenOptions is the base configuration which may be used in net listeners
type ListenOptions struct {
	Addr                string
	ReusePort           bool `cfg:"reuse_port"`
	Workers             uint
	EnableRequestTiming bool `cfg:"enable_request_timing"`
}

// Validate checks if listen address provided and sets default
// timeout options if needed
func (lo ListenOptions) Validate(logger *log.Logger) (validOptions ListenOptions) {
	validOptions = lo
	if len(lo.Addr) == 0 {
		validOptions.Addr = DefaultListenAddress
		logger.Warn().
			Str("name", "Addr").
			Str("provided", lo.Addr).
			Str("default", validOptions.Addr).
			Msg("falling back to default configuration")
	}
	return
}

// ListenTCP listens at the given TCP Addr
// with SO_REUSEPORT and SO_REUSEADDR options enabled if
// ReusePort set to true
func (lo ListenOptions) ListenTCP() (conn *net.TCPListener, err error) {
	if lo.ReusePort && reuseport.Available() {
		var ln net.Listener
		if ln, err = reuseport.Listen("tcp", lo.Addr); err == nil {
			var ok bool
			if conn, ok = ln.(*net.TCPListener); !ok {
				err = errUnexpectedListenerType
			}
		}
	} else {
		var addr *net.TCPAddr
		if addr, err = net.ResolveTCPAddr("tcp", lo.Addr); err == nil {
			conn, err = net.ListenTCP("tcp", addr)
		}
	}
	return
}

// ListenUDP listens at the given UDP Addr
// with SO_REUSEPORT and SO_REUSEADDR options enabled if
// ReusePort set to true
func (lo ListenOptions) ListenUDP() (conn *net.UDPConn, err error) {
	if lo.ReusePort && reuseport.Available() {
		var ln net.PacketConn
		if ln, err = reuseport.ListenPacket("udp", lo.Addr); err == nil {
			var ok bool
			if conn, ok = ln.(*net.UDPConn); !ok {
				err = errUnexpectedListenerType
			}
		}
	} else {
		var addr *net.UDPAddr
		if addr, err = net.ResolveUDPAddr("udp", lo.Addr); err == nil {
			conn, err = net.ListenUDP("udp", addr)
		}
	}
	return
}

// ParseOptions is the configuration used to parse an Announce Request.
//
// If AllowIPSpoofing is true, IPs provided via params will be used.
type ParseOptions struct {
	AllowIPSpoofing     bool   `cfg:"allow_ip_spoofing"`
	FilterPrivateIPs    bool   `cfg:"filter_private_ips"`
	MaxNumWant          uint32 `cfg:"max_numwant"`
	DefaultNumWant      uint32 `cfg:"default_numwant"`
	MaxScrapeInfoHashes uint32 `cfg:"max_scrape_infohashes"`
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
func (op ParseOptions) Validate(logger *log.Logger) ParseOptions {
	valid := op
	if op.MaxNumWant <= 0 {
		valid.MaxNumWant = defaultMaxNumWant
		logger.Warn().
			Str("name", "MaxNumWant").
			Uint32("provided", op.MaxNumWant).
			Uint32("default", valid.MaxNumWant).
			Msg("falling back to default configuration")
	}

	if op.DefaultNumWant <= 0 {
		valid.DefaultNumWant = defaultDefaultNumWant
		logger.Warn().
			Str("name", "DefaultNumWant").
			Uint32("provided", op.DefaultNumWant).
			Uint32("default", valid.DefaultNumWant).
			Msg("falling back to default configuration")
	}

	if op.MaxScrapeInfoHashes <= 0 {
		valid.MaxScrapeInfoHashes = defaultMaxScrapeInfoHashes
		logger.Warn().
			Str("name", "MaxScrapeInfoHashes").
			Uint32("provided", op.MaxScrapeInfoHashes).
			Uint32("default", valid.MaxScrapeInfoHashes).
			Msg("falling back to default configuration")
	}
	return valid
}

// Default parser config constants.
const (
	defaultMaxNumWant          = 100
	defaultDefaultNumWant      = 50
	defaultMaxScrapeInfoHashes = 50
)
