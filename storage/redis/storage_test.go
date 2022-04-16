package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis"

	s "github.com/sot-tech/mochi/storage"
	"github.com/sot-tech/mochi/storage/test"
)

var cfg = Config{
	GarbageCollectionInterval:   10 * time.Minute,
	PrometheusReportingInterval: 10 * time.Minute,
	PeerLifetime:                30 * time.Minute,
	ReadTimeout:                 10 * time.Second,
	WriteTimeout:                10 * time.Second,
	ConnectTimeout:              10 * time.Second,
}

func createNew() s.PeerStorage {
	var ps s.PeerStorage
	var err error
	ps, err = New(cfg)
	if err != nil {
		fmt.Println("unable to create real Redis connection: ", err, " using simulator")
		var rs *miniredis.Miniredis
		rs, err = miniredis.Run()
		if err != nil {
			panic(err)
		}
		cfg.Addresses = []string{rs.Addr()}
		ps, err = New(cfg)
	}
	if err != nil {
		panic(err)
	}
	return ps
}

func TestStorage(t *testing.T) { test.RunTests(t, createNew()) }

func BenchmarkStorage(b *testing.B) { test.RunBenchmarks(b, createNew) }
