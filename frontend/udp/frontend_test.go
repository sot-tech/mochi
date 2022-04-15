package udp_test

import (
	"testing"

	"github.com/sot-tech/mochi/frontend/udp"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	_ "github.com/sot-tech/mochi/pkg/randseed"
	"github.com/sot-tech/mochi/storage"
	_ "github.com/sot-tech/mochi/storage/memory"
)

func TestStartStopRaceIssue437(t *testing.T) {
	ps, err := storage.NewStorage("memory", conf.MapConfig{})
	if err != nil {
		t.Fatal(err)
	}
	lgc := middleware.NewLogic(0, 0, ps, nil, nil)
	fe, err := udp.NewFrontend(lgc, conf.MapConfig{"addr": "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	errC := fe.Stop()
	if errs := <-errC; len(errs) != 0 {
		t.Fatal(errs)
	}
}
