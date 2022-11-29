package udp

import (
	"crypto/hmac"
	"encoding/binary"
	"hash"
	"net/netip"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/sot-tech/mochi/pkg/log"
)

// ttl is the duration a connection ID should be valid according to BEP 15.
const ttl = 2 * time.Minute

// A ConnectionIDGenerator is a reusable generator and validator for connection
// IDs as described in BEP 15.
// It is not thread safe, but is safe to be pooled and reused by other
// goroutines. It manages its state itself, so it can be taken from and returned
// to a pool without any cleanup.
// After initial creation, it can generate connection IDs without allocating.
// See Generate and Validate for usage notes and guarantees.
type ConnectionIDGenerator struct {
	// mac is a keyed HMAC that can be reused for subsequent connection ID
	// generations.
	mac hash.Hash

	// connID is an 8-byte slice that holds the generated connection ID after a
	// call to Generate.
	// It must not be referenced after the generator is returned to a pool.
	// It will be overwritten by subsequent calls to Generate.
	connID []byte

	// the leeway for a timestamp on a connection ID.
	maxClockSkew time.Duration
}

// NewConnectionIDGenerator creates a new connection ID generator.
func NewConnectionIDGenerator(key string, maxClockSkew time.Duration) *ConnectionIDGenerator {
	return &ConnectionIDGenerator{
		mac: hmac.New(func() hash.Hash {
			return xxh3.New()
		}, []byte(key)),
		connID:       make([]byte, 8),
		maxClockSkew: maxClockSkew,
	}
}

// Generate generates an 8-byte connection ID as described in BEP 15 for the
// given IP and the current time.
//
// The first 4 bytes of the connection identifier is a unix timestamp and the
// last 4 bytes are a truncated HMAC token created from the aforementioned
// unix timestamp and the source IP address of the UDP packet.
//
// Truncated HMAC is known to be safe for 2^(-n) where n is the size in bits
// of the truncated HMAC token. In this use case we have 32 bits, thus a
// forgery probability of approximately 1 in 4 billion.
//
// The generated ID is written to g.connID, which is also returned. g.connID
// will be reused, so it must not be referenced after returning the generator
// to a pool and will be overwritten be subsequent calls to Generate!
func (g *ConnectionIDGenerator) Generate(ip netip.Addr, now time.Time) []byte {
	g.mac.Reset()
	binary.BigEndian.PutUint32(g.connID, uint32(now.Unix()))

	g.mac.Write(g.connID[:4])
	g.mac.Write(ip.AsSlice())
	copy(g.connID[4:8], g.mac.Sum(nil)[:4])

	log.Debug().
		Stringer("ip", ip).
		Time("now", now).
		Hex("connID", g.connID).
		Msg("generated connection ID")
	return g.connID
}

// Validate validates the given connection ID for an IP and the current time.
func (g *ConnectionIDGenerator) Validate(connectionID []byte, ip netip.Addr, now time.Time) bool {
	g.mac.Reset()
	tsBytes := connectionID[:4]
	ts := time.Unix(int64(binary.BigEndian.Uint32(tsBytes)), 0)
	log.Debug().
		Stringer("ip", ip).
		Time("ts", ts).
		Time("now", now).
		Hex("connID", g.connID).
		Msg("validating connection ID")

	g.mac.Write(tsBytes)
	g.mac.Write(ip.AsSlice())
	return hmac.Equal(g.mac.Sum(nil)[:4], connectionID[4:8]) &&
		now.Before(ts.Add(ttl)) &&
		ts.Before(now.Add(g.maxClockSkew))
}
