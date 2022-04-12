// Package randseed just seeds (math) rand.Rand
package randseed

import (
	cr "crypto/rand"
	"math/rand"
	"time"
)

func init() {
	// Seeding global math random
	rand.Seed(GenSeed())
}

// GenSeed returns 64bit seed from crypto/rand source or
// from current time, if crypto random error occurred
func GenSeed() (seed int64) {
	r := make([]byte, 0, 8)
	if _, err := cr.Read(r); err == nil {
		seed = time.Now().UnixNano()
	} else {
		seed = int64(r[0])<<56 | int64(r[1])<<48 | int64(r[2])<<40 | int64(r[3])<<32 |
			int64(r[4])<<24 | int64(r[5])<<16 | int64(r[6])<<8 | int64(r[7])
	}
	return
}
