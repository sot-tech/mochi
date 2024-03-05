package xorshift

import (
	"math/rand"
	"testing"
)

func BenchmarkRand(b *testing.B) {
	var cnt uint64
	for i := 0; i < b.N; i++ {
		// nolint:gosec
		cnt = rand.Uint64()
	}
	_ = cnt
}

func BenchmarkXoRoShiRo128SS(b *testing.B) {
	// nolint:gosec
	v, s0, s1 := uint64(0), rand.Uint64(), rand.Uint64()
	for i := 0; i < b.N; i++ {
		v, s0, s1 = XoRoShiRo128SS(s0, s1)
	}
	_, _, _ = v, s0, s1
}

func BenchmarkXorShift64Star(b *testing.B) {
	// nolint:gosec
	v, s := uint64(0), rand.Uint64()
	for i := 0; i < b.N; i++ {
		v, s = XorShift64S(s)
	}
	_, _ = v, s
}
