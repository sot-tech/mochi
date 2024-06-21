package mdb

import (
	"fmt"
	s "github.com/sot-tech/mochi/storage"
	"github.com/sot-tech/mochi/storage/test"
	"os"
	"testing"
)

const tmpPath = ""

var cfg = config{
	Path:        "",
	Mode:        defaultMode,
	DataDBName:  "KV",
	PeersDBName: "PEERS",
	MaxSize:     defaultMapSize,
	MaxReaders:  defaultMaxReaders,
	AsyncWrite:  true,
	NoMetaSync:  false,
}

func createNew() s.PeerStorage {
	var ps s.PeerStorage
	var err error
	ps, err = newStorage(cfg)
	if err != nil {
		panic(fmt.Sprint("Unable to open/create LMDB: ", err))
	}
	return ps
}

func TestStorage(t *testing.T) {
	tmpDir, err := os.MkdirTemp(tmpPath, "lmdb*")
	if err != nil {
		t.Error(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
		}
	})
	cfg.Path = tmpDir
	test.RunTests(t, createNew())
}

func BenchmarkStorage(b *testing.B) {
	tmpDir, err := os.MkdirTemp(tmpPath, "lmdb*")
	if err != nil {
		b.Error(err)
	}
	b.Cleanup(func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
		}
	})
	cfg.Path = tmpDir
	test.RunBenchmarks(b, createNew)
}
