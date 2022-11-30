package udp

import (
	"crypto/hmac"
	"encoding/binary"
	"fmt"
	"hash"
	"math/rand"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/sot-tech/mochi/pkg/log"
	_ "github.com/sot-tech/mochi/pkg/randseed"
	"github.com/stretchr/testify/require"
)

var golden = []struct {
	createdAt int64
	now       int64
	ip        string
	key       []byte
	valid     bool
}{
	{0, 1, "127.0.0.1", []byte(""), true},
	{0, 420420, "127.0.0.1", []byte(""), false},
	{0, 0, "::1", []byte(""), true},
}

// NewConnectionID creates an 8-byte connection identifier for UDP packets as
// described by BEP 15.
// This is a wrapper around creating a new ConnectionIDGenerator and generating
// an ID. It is recommended to use the generator for performance.
func NewConnectionID(ip netip.Addr, now time.Time, key []byte) []byte {
	return NewConnectionIDGenerator(key, 0).Generate(ip, now)
}

// ValidConnectionID determines whether a connection identifier is legitimate.
// This is a wrapper around creating a new ConnectionIDGenerator and validating
// the ID. It is recommended to use the generator for performance.
func ValidConnectionID(connectionID []byte, ip netip.Addr, now time.Time, maxClockSkew time.Duration, key []byte) bool {
	return NewConnectionIDGenerator(key, maxClockSkew).Validate(connectionID, ip, now)
}

// simpleNewConnectionID generates a new connection ID the explicit way.
// This is used to verify correct behaviour of the generator.
func simpleNewConnectionID(ip netip.Addr, now time.Time, key []byte) []byte {
	buffer := make([]byte, 9)
	mac := hmac.New(func() hash.Hash {
		return xxhash.New()
	}, key)
	buffer[0] = byte(rand.Int())
	binary.BigEndian.PutUint64(buffer[1:], uint64(now.Unix()))
	mac.Write(buffer)
	mac.Write(ip.AsSlice())
	buffer[1], buffer[2] = buffer[7], buffer[8]
	copy(buffer[3:8], mac.Sum(nil))
	buffer = buffer[:8]

	// this is just in here because logging impacts performance and we benchmark
	// this version too.
	log.Debug().
		Stringer("ip", ip).
		Time("ts", now).
		Hex("connID", buffer).
		Msg("generated connection ID")
	return buffer
}

func TestVerification(t *testing.T) {
	for _, tt := range golden {
		t.Run(fmt.Sprintf("%s created at %d verified at %d", tt.ip, tt.createdAt, tt.now), func(t *testing.T) {
			cid := NewConnectionID(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0), tt.key)
			got := ValidConnectionID(cid, netip.MustParseAddr(tt.ip), time.Unix(tt.now, 0), time.Minute, tt.key)
			if got != tt.valid {
				t.Errorf("expected validity: %t got validity: %t", tt.valid, got)
			}
		})
	}
}

func TestGeneration(t *testing.T) {
	for _, tt := range golden {
		t.Run(fmt.Sprintf("%s created at %d", tt.ip, tt.createdAt), func(t *testing.T) {
			want := simpleNewConnectionID(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0), tt.key)
			got := NewConnectionID(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0), tt.key)
			require.NotEqual(t, want, got) // IDs should NOT be equal because of salt
		})
	}
}

func TestReuseGeneratorGenerate(t *testing.T) {
	for _, tt := range golden {
		t.Run(fmt.Sprintf("%s created at %d", tt.ip, tt.createdAt), func(t *testing.T) {
			cid := NewConnectionID(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0), tt.key)
			require.Len(t, cid, 8)

			gen := NewConnectionIDGenerator(tt.key, 0)

			for i := 0; i < 3; i++ {
				connID := gen.Generate(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0))
				require.Equal(t, cid, connID) // IDs should NOT be equal because of salt
			}
		})
	}
}

func TestReuseGeneratorValidate(t *testing.T) {
	for _, tt := range golden {
		t.Run(fmt.Sprintf("%s created at %d verified at %d", tt.ip, tt.createdAt, tt.now), func(t *testing.T) {
			gen := NewConnectionIDGenerator(tt.key, time.Minute)
			cid := gen.Generate(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0))
			for i := 0; i < 3; i++ {
				got := gen.Validate(cid, netip.MustParseAddr(tt.ip), time.Unix(tt.now, 0))
				if got != tt.valid {
					t.Errorf("expected validity: %t got validity: %t", tt.valid, got)
				}
			}
		})
	}
}

func BenchmarkSimpleNewConnectionID(b *testing.B) {
	ip := netip.MustParseAddr("127.0.0.1")
	key := []byte("some random string that is hopefully at least this long")
	createdAt := time.Now()

	b.RunParallel(func(pb *testing.PB) {
		sum := int64(0)

		for pb.Next() {
			cid := simpleNewConnectionID(ip, createdAt, key)
			sum += int64(cid[7])
		}

		_ = sum
	})
}

func BenchmarkNewConnectionID(b *testing.B) {
	ip := netip.MustParseAddr("127.0.0.1")
	key := []byte("some random string that is hopefully at least this long")
	createdAt := time.Now()

	b.RunParallel(func(pb *testing.PB) {
		sum := int64(0)

		for pb.Next() {
			cid := NewConnectionID(ip, createdAt, key)
			sum += int64(cid[7])
		}

		_ = sum
	})
}

func BenchmarkConnectionIDGenerator_Generate(b *testing.B) {
	ip := netip.MustParseAddr("127.0.0.1")
	key := []byte("some random string that is hopefully at least this long")
	createdAt := time.Now()

	pool := &sync.Pool{
		New: func() any {
			return NewConnectionIDGenerator(key, 0)
		},
	}

	b.RunParallel(func(pb *testing.PB) {
		sum := int64(0)
		for pb.Next() {
			gen := pool.Get().(*ConnectionIDGenerator)
			cid := gen.Generate(ip, createdAt)
			sum += int64(cid[7])
			pool.Put(gen)
		}
	})
}

func BenchmarkValidConnectionID(b *testing.B) {
	ip := netip.MustParseAddr("127.0.0.1")
	key := []byte("some random string that is hopefully at least this long")
	createdAt := time.Now()
	cid := NewConnectionID(ip, createdAt, key)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !ValidConnectionID(cid, ip, createdAt, 10*time.Second, key) {
				b.FailNow()
			}
		}
	})
}

func BenchmarkConnectionIDGenerator_Validate(b *testing.B) {
	ip := netip.MustParseAddr("127.0.0.1")
	key := []byte("some random string that is hopefully at least this long")
	createdAt := time.Now()
	cid := NewConnectionID(ip, createdAt, key)

	pool := &sync.Pool{
		New: func() any {
			return NewConnectionIDGenerator(key, 10*time.Second)
		},
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			gen := pool.Get().(*ConnectionIDGenerator)
			if !gen.Validate(cid, ip, createdAt) {
				b.FailNow()
			}
			pool.Put(gen)
		}
	})
}
