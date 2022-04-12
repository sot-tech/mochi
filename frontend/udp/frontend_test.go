package udp_test

import (
	"testing"

	"github.com/sot-tech/mochi/frontend/udp"
	"github.com/sot-tech/mochi/middleware"
	_ "github.com/sot-tech/mochi/pkg/rand_seed"
	"github.com/sot-tech/mochi/storage"
	_ "github.com/sot-tech/mochi/storage/memory"
)

func TestStartStopRaceIssue437(t *testing.T) {
	ps, err := storage.NewStorage("memory", nil)
	if err != nil {
		t.Fatal(err)
	}
	var responseConfig middleware.ResponseConfig
	lgc := middleware.NewLogic(responseConfig, ps, nil, nil)
	fe, err := udp.NewFrontend(lgc, udp.Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	errC := fe.Stop()
	errs := <-errC
	if len(errs) != 0 {
		t.Fatal(errs[0])
	}
}
