package udp_test

import (
	"testing"

	"github.com/sot-tech/mochi/frontend/udp"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
	_ "github.com/sot-tech/mochi/storage/memory"
)

func init() {
	_ = log.ConfigureLogger("", "warn", false, false)
}

func TestStartStopRaceIssue437(t *testing.T) {
	ps, err := storage.NewPeerStorage(conf.NamedMapConfig{
		Name:   "memory",
		Config: conf.MapConfig{},
	})
	if err != nil {
		t.Fatal(err)
	}
	lgc := middleware.NewLogic(0, 0, ps, nil, nil)
	fe, err := udp.NewFrontend(conf.MapConfig{"addr": "127.0.0.1:0"}, lgc)
	if err != nil {
		t.Fatal(err)
	}
	if err = fe.Close(); err != nil {
		t.Fatal(err)
	}
}
