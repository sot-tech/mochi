package memory

import (
	"github.com/chihaya/chihaya/storage/test"
	"testing"
	"time"

	s "github.com/chihaya/chihaya/storage"
)

func createNew() s.Storage {
	ps, err := New(Config{
		ShardCount:                  1024,
		GarbageCollectionInterval:   10 * time.Minute,
		PrometheusReportingInterval: 10 * time.Minute,
		PeerLifetime:                30 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
	return ps
}

func TestStorage(t *testing.T) { test.RunTests(t, createNew()) }

func BenchmarkStorage(b *testing.B) { test.RunBenchmarks(b, createNew) }
