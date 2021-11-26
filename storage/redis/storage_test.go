package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	s "github.com/chihaya/chihaya/storage"
	"github.com/chihaya/chihaya/storage/test"
)

func createNew() s.Storage {
	rs, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	redisURL := fmt.Sprintf("redis://@%s/0", rs.Addr())
	ps, err := New(Config{
		GarbageCollectionInterval:   10 * time.Minute,
		PrometheusReportingInterval: 10 * time.Minute,
		PeerLifetime:                30 * time.Minute,
		RedisBroker:                 redisURL,
		RedisReadTimeout:            10 * time.Second,
		RedisWriteTimeout:           10 * time.Second,
		RedisConnectTimeout:         10 * time.Second})
	if err != nil {
		panic(err)
	}
	return ps
}

func TestStorage(t *testing.T) { test.RunTests(t, createNew()) }

func BenchmarkStorage(b *testing.B) { test.RunBenchmarks(b, createNew) }
