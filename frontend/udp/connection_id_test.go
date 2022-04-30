package udp

import (
	"crypto/hmac"
	"encoding/binary"
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/minio/sha256-simd"
	"github.com/stretchr/testify/require"

	"github.com/sot-tech/mochi/pkg/log"
)

var golden = []struct {
	createdAt int64
	now       int64
	ip        string
	key       string
	valid     bool
}{
	{0, 1, "127.0.0.1", "", true},
	{0, 420420, "127.0.0.1", "", false},
	{0, 0, "::1", "", true},
}

// NewConnectionID creates an 8-byte connection identifier for UDP packets as
// described by BEP 15.
// This is a wrapper around creating a new ConnectionIDGenerator and generating
// an ID. It is recommended to use the generator for performance.
func NewConnectionID(ip netip.Addr, now time.Time, key string) []byte {
	return NewConnectionIDGenerator(key).Generate(ip, now)
}

// ValidConnectionID determines whether a connection identifier is legitimate.
// This is a wrapper around creating a new ConnectionIDGenerator and validating
// the ID. It is recommended to use the generator for performance.
func ValidConnectionID(connectionID []byte, ip netip.Addr, now time.Time, maxClockSkew time.Duration, key string) bool {
	return NewConnectionIDGenerator(key).Validate(connectionID, ip, now, maxClockSkew)
}

// simpleNewConnectionID generates a new connection ID the explicit way.
// This is used to verify correct behaviour of the generator.
func simpleNewConnectionID(ip netip.Addr, now time.Time, key string) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf, uint32(now.Unix()))

	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(buf[:4])
	ipBytes, _ := ip.MarshalBinary()
	mac.Write(ipBytes)
	macBytes := mac.Sum(nil)[:4]
	copy(buf[4:], macBytes)

	// this is just in here because logging impacts performance and we benchmark
	// this version too.
	log.Debug().
		Stringer("ip", ip).
		Time("now", now).
		Bytes("connID", buf).
		Msg("manually generated connection ID")
	return buf
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
			require.Equal(t, want, got)
		})
	}
}

func TestReuseGeneratorGenerate(t *testing.T) {
	for _, tt := range golden {
		t.Run(fmt.Sprintf("%s created at %d", tt.ip, tt.createdAt), func(t *testing.T) {
			cid := NewConnectionID(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0), tt.key)
			require.Len(t, cid, 8)

			gen := NewConnectionIDGenerator(tt.key)

			for i := 0; i < 3; i++ {
				connID := gen.Generate(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0))
				require.Equal(t, cid, connID)
			}
		})
	}
}

func TestReuseGeneratorValidate(t *testing.T) {
	for _, tt := range golden {
		t.Run(fmt.Sprintf("%s created at %d verified at %d", tt.ip, tt.createdAt, tt.now), func(t *testing.T) {
			gen := NewConnectionIDGenerator(tt.key)
			cid := gen.Generate(netip.MustParseAddr(tt.ip), time.Unix(tt.createdAt, 0))
			for i := 0; i < 3; i++ {
				got := gen.Validate(cid, netip.MustParseAddr(tt.ip), time.Unix(tt.now, 0), time.Minute)
				if got != tt.valid {
					t.Errorf("expected validity: %t got validity: %t", tt.valid, got)
				}
			}
		})
	}
}

func BenchmarkSimpleNewConnectionID(b *testing.B) {
	ip := netip.MustParseAddr("127.0.0.1")
	key := "some random string that is hopefully at least this long"
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
	key := "some random string that is hopefully at least this long"
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
	key := "some random string that is hopefully at least this long"
	createdAt := time.Now()

	pool := &sync.Pool{
		New: func() any {
			return NewConnectionIDGenerator(key)
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
	key := "some random string that is hopefully at least this long"
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
	key := "some random string that is hopefully at least this long"
	createdAt := time.Now()
	cid := NewConnectionID(ip, createdAt, key)

	pool := &sync.Pool{
		New: func() any {
			return NewConnectionIDGenerator(key)
		},
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			gen := pool.Get().(*ConnectionIDGenerator)
			if !gen.Validate(cid, ip, createdAt, 10*time.Second) {
				b.FailNow()
			}
			pool.Put(gen)
		}
	})
}
