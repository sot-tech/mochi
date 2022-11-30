package udp

import (
	"crypto/hmac"
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"hash"
	"math/rand"
	"net/netip"
	"time"

	"github.com/sot-tech/mochi/pkg/log"
)

// ttl is the duration a connection ID should be valid according to BEP 15.
var ttl = int64(2 * time.Minute)

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

	buff []byte

	// scratch is a 32-byte slice that is used as a scratchpad for the generated
	// HMACs.
	scratch []byte

	// the leeway for a timestamp on a connection ID.
	maxClockSkew int64

	cnt uint64
}

// NewConnectionIDGenerator creates a new connection ID generator.
func NewConnectionIDGenerator(key string, maxClockSkew time.Duration) *ConnectionIDGenerator {
	return &ConnectionIDGenerator{
		mac: hmac.New(func() hash.Hash {
			return xxhash.New()
		}, []byte(key)),
		connID:       make([]byte, 8),
		buff:         make([]byte, 9),
		scratch:      make([]byte, 16),
		maxClockSkew: int64(maxClockSkew),
		cnt:          rand.Uint64(),
	}
}

// xor-shift-star generator
func (g *ConnectionIDGenerator) nextCnt() uint64 {
	g.cnt ^= g.cnt >> 12
	g.cnt ^= g.cnt << 25
	g.cnt ^= g.cnt >> 27
	g.cnt *= 0x2545F4914F6CDD1D
	return g.cnt
}

// reset resets the generator.
// This is called by other methods of the generator, it's not necessary to call
// it after getting a generator from a pool.
func (g *ConnectionIDGenerator) reset() {
	g.mac.Reset()
	g.connID = g.connID[:8]
	g.buff = g.buff[:9]
	g.scratch = g.scratch[:0]
}

// Generate generates an 8-byte connection ID as described in BEP 15 for the
// given IP and the current time.
//
// The first byte is random salt, next 2 bytes - truncated unix timestamp
// when ID was generated, last 5 bytes are a truncated HMAC token created
// from salt (1 byte), full unix timestamp (8 bytes) and source IP (4/16 bytes).
// Salt used to mitigate generation same MAC if there are several clients
// from same IP sent requests within one second.
//
// Truncated HMAC is known to be safe for 2^(-n) where n is the size in bits
// of the truncated HMAC token. In this use case we have 40 bits, thus a
// forgery probability of approximately 1 in 4 billion.
//
// The generated ID is written to g.buffer, which is also returned. g.buffer
// will be reused, so it must not be referenced after returning the generator
// to a pool and will be overwritten be subsequent calls to Generate!
func (g *ConnectionIDGenerator) Generate(ip netip.Addr, now time.Time) (out []byte) {
	g.reset()
	g.buff[0] = byte(g.nextCnt())
	binary.BigEndian.PutUint64(g.buff[1:], uint64(now.Unix()))
	g.mac.Write(g.buff)
	g.mac.Write(ip.AsSlice())

	g.scratch = g.mac.Sum(g.scratch)
	g.connID[0], g.connID[1], g.connID[2] = g.buff[0], g.buff[7], g.buff[8]
	copy(g.connID[3:], g.scratch[:5])

	log.Debug().
		Stringer("ip", ip).
		Hex("connID", g.connID).
		Msg("generated connection ID")
	return g.connID[:8]
}

// Validate validates the given connection ID for an IP and the current time.
func (g *ConnectionIDGenerator) Validate(connectionID []byte, ip netip.Addr, now time.Time) bool {
	g.reset()
	nowTS := now.Unix()
	g.buff[0] = connectionID[0]
	ts := nowTS&((^int64(0)>>16)<<16) | int64(connectionID[1])<<8 | int64(connectionID[2])
	binary.BigEndian.PutUint64(g.buff[1:], uint64(ts))
	g.mac.Write(g.buff)
	g.mac.Write(ip.AsSlice())
	g.scratch = g.mac.Sum(g.scratch)
	res := hmac.Equal(g.scratch[:5], connectionID[3:8])
	res = ts-g.maxClockSkew <= nowTS && res
	res = nowTS < ts+ttl+g.maxClockSkew && res
	log.Debug().
		Stringer("ip", ip).
		Hex("connID", connectionID).
		Bool("result", res).
		Msg("validating connection ID")
	return res
}
