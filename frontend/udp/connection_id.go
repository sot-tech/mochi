package udp

import (
	"crypto/hmac"
	cr "crypto/rand"
	"encoding/binary"
	"hash"
	"net/netip"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/xorshift"
)

// ttl is the duration a connection ID should be valid according to BEP 15.
var ttl = int64(2 * time.Minute)

const (
	// length of connection ID
	connIDLen = 8
	// uint64 length + 1 byte salt
	buffLen = 9
	// 16 bytes enough for hashes with output length up to 128bit
	scratchLen = 16
	// length of HMAC in bytes to place it in connection ID
	hmacLen = 5
)

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

	// buffer for HMAC input
	buff []byte

	// scratch is a 32-byte slice that is used as a scratchpad for the generated
	// HMACs to increase hash performance.
	scratch []byte

	// the leeway for a timestamp on a connection ID.
	maxClockSkew int64

	// PRNG footprint holder
	s uint64
}

// NewConnectionIDGenerator creates a new connection ID generator.
func NewConnectionIDGenerator(key []byte, maxClockSkew time.Duration) *ConnectionIDGenerator {
	return &ConnectionIDGenerator{
		mac: hmac.New(func() hash.Hash {
			return xxhash.New()
		}, key),
		connID:       make([]byte, connIDLen),
		buff:         make([]byte, buffLen),
		scratch:      make([]byte, scratchLen),
		maxClockSkew: int64(maxClockSkew),
	}
}

// reset resets the generator.
// This is called by other methods of the generator, it's not necessary to call
// it after getting a generator from a pool.
func (g *ConnectionIDGenerator) reset(init bool) {
	g.mac.Reset()
	g.connID = g.connID[:connIDLen]
	g.buff = g.buff[:buffLen]
	g.scratch = g.scratch[:0]
	if init {
		r := make([]byte, 8)
		if _, err := cr.Read(r); err == nil {
			g.s = binary.BigEndian.Uint64(r)
		} else {
			g.s = uint64(time.Now().UnixNano())
		}
	}
}

// Generate generates an 8-byte connection ID as described in BEP 15 for the
// given IP and the current time.
//
// The first byte is random salt, next 2 bytes - truncated unix timestamp
// when ID was generated, last 5 bytes are a truncated HMAC token created
// from salt (1 byte), full unix timestamp (8 bytes) and source IP (4/16 bytes).
//
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
	g.reset(true)
	var r uint64
	r, g.s = xorshift.XorShift64S(g.s)
	g.buff[0] = byte(r)
	binary.BigEndian.PutUint64(g.buff[1:], uint64(now.Unix()))
	g.mac.Write(g.buff)
	g.mac.Write(ip.AsSlice())

	g.scratch = g.mac.Sum(g.scratch)
	g.connID[0], g.connID[1], g.connID[2] = g.buff[0], g.buff[7], g.buff[8]
	copy(g.connID[connIDLen-hmacLen:], g.scratch[:hmacLen])

	log.Debug().
		Stringer("ip", ip).
		Hex("connID", g.connID).
		Msg("generated connection ID")
	return g.connID[:connIDLen]
}

// Validate validates the given connection ID for an IP and the current time.
func (g *ConnectionIDGenerator) Validate(connectionID []byte, ip netip.Addr, now time.Time) bool {
	g.reset(false)
	nowTS := now.Unix()
	g.buff[0] = connectionID[0]
	// connectionID contains only 2 bytes of timestamp, so we clean little 16 bits to place it and rehash.
	// We will provide restored full timestamp respectively to current timestamp,
	// 2 bytes should be enough to avoid collisions within ~18 hours from same IP.
	ts := nowTS&((^int64(0)>>16)<<16) | int64(connectionID[1])<<8 | int64(connectionID[2])
	binary.BigEndian.PutUint64(g.buff[1:], uint64(ts))
	g.mac.Write(g.buff)
	g.mac.Write(ip.AsSlice())
	g.scratch = g.mac.Sum(g.scratch)
	res := hmac.Equal(g.scratch[:hmacLen], connectionID[connIDLen-hmacLen:connIDLen])
	// ts-skew < now < ts+ttl+skew
	res = ts-g.maxClockSkew < nowTS && res
	res = nowTS < ts+ttl+g.maxClockSkew && res
	log.Debug().
		Stringer("ip", ip).
		Hex("connID", connectionID).
		Bool("result", res).
		Msg("validating connection ID")
	return res
}
