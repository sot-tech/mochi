// Package xorshift contains functions for fast generating
// predictable pseudorandom numbers
// See https://prng.di.unimi.it .
package xorshift

// XoRoShiRo128SS calculates predictable pseudorandom number
// with XOR/rotate/shift/rotate 128** (xoroshiro128starstar) algorithm.
// In some cases a little faster than XorShift64S, but uses 128 bits footprint.
// see https://prng.di.unimi.it/xoroshiro128starstar.c
func XoRoShiRo128SS(s0, s1 uint64) (uint64, uint64, uint64) {
	r := s0 * 5
	r = ((r << 7) | (r >> 57)) * 9 // rotl(s0*5, 7) * 9
	s1 ^= s0
	s0 = ((s0 << 24) | (s0 >> 40)) ^ s1 ^ (s1 << 16) // rotl(s0, 24) ^ s1 ^ (s1 << 16)
	s1 = (s1 << 37) | (s1 >> 27)                     // rotl(s1, 37)
	return r, s0, s1
}

// XorShift64S calculates predictable pseudorandom number
// with XOR/Shift 64* (shorshift64*) algorithm.
// see https://vigna.di.unimi.it/ftp/papers/xorshift.pdf
func XorShift64S(s uint64) (uint64, uint64) {
	s ^= s >> 12
	s ^= s << 25
	s ^= s >> 27
	return s * uint64(0x2545F4914F6CDD1D), s
}
