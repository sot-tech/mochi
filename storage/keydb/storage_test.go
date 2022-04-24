package keydb

import (
	"fmt"
	"testing"
	"time"

	s "github.com/sot-tech/mochi/storage"
	r "github.com/sot-tech/mochi/storage/redis"
	"github.com/sot-tech/mochi/storage/test"
)

var cfg = r.Config{
	Addresses:      []string{"localhost:6379"},
	PeerLifetime:   30 * time.Minute,
	ReadTimeout:    10 * time.Second,
	WriteTimeout:   10 * time.Second,
	ConnectTimeout: 10 * time.Second,
}

func createNew() s.PeerStorage {
	var ps s.PeerStorage
	var err error
	ps, err = newStore(cfg)
	if err != nil {
		panic(fmt.Sprint("Unable to create KeyDB connection: ", err, "\nThis driver needs real KeyDB instance"))
	}
	return ps
}

func TestStorage(t *testing.T) { test.RunTests(t, createNew()) }

func BenchmarkStorage(b *testing.B) { test.RunBenchmarks(b, createNew) }
