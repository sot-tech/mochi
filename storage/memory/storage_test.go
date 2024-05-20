package memory

import (
	"testing"

	"github.com/sot-tech/mochi/storage"
	"github.com/sot-tech/mochi/storage/test"
)

func createNew() storage.PeerStorage {
	ps, err := peerStorage(config{ShardCount: 1024})
	if err != nil {
		panic(err)
	}
	return ps
}

func TestStorage(t *testing.T) { test.RunTests(t, createNew()) }

func BenchmarkStorage(b *testing.B) { test.RunBenchmarks(b, createNew) }
