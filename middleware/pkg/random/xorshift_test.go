package random

import (
	_ "github.com/sot-tech/mochi/pkg/rand_seed"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestIntn(t *testing.T) {
	s0, s1 := rand.Uint64(), rand.Uint64()
	var k int
	for i := 0; i < 10000; i++ {
		k, s0, s1 = Intn(s0, s1, 10)
		require.True(t, k >= 0, "Intn() must be >= 0")
		require.True(t, k < 10, "Intn(k) must be < k")
	}
}

func BenchmarkAdvanceXORShift128Plus(b *testing.B) {
	s0, s1 := rand.Uint64(), rand.Uint64()
	var v uint64
	for i := 0; i < b.N; i++ {
		v, s0, s1 = GenerateAndAdvance(s0, s1)
	}
	_, _, _ = v, s0, s1
}

func BenchmarkIntn(b *testing.B) {
	s0, s1 := rand.Uint64(), rand.Uint64()
	var v int
	for i := 0; i < b.N; i++ {
		v, s0, s1 = Intn(s0, s1, 1000)
	}
	_, _, _ = v, s0, s1
}
