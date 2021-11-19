// Package torrentapproval implements a Hook that fails an Announce based on a
// whitelist or blacklist of torrent hash.
package torrentapproval

import (
	"context"
	"fmt"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"github.com/chihaya/chihaya/pkg/stop"
	"github.com/chihaya/chihaya/storage"
	"gopkg.in/yaml.v2"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/middleware"
	_ "github.com/chihaya/chihaya/middleware/torrentapproval/container/directory"
	_ "github.com/chihaya/chihaya/middleware/torrentapproval/container/list"
)

// Name is the name by which this middleware is registered with Chihaya.
const Name = "torrent approval"

func init() {
	middleware.RegisterDriver(Name, driver{})
}

type driver struct{}

func (d driver) NewHook(optionBytes []byte, storage storage.Storage) (middleware.Hook, error) {
	var cfg middleware.Config
	err := yaml.Unmarshal(optionBytes, &cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid options for middleware %s: %s", Name, err)
	}

	if len(cfg.Name) == 0 {
		return nil, fmt.Errorf("invalid options for middleware %s: name not provided", Name)
	}

	if cfg.Options == nil {
		return nil, fmt.Errorf("invalid options for middleware %s: options not provided", Name)
	}

	var confBytes []byte
	if confBytes, err = yaml.Marshal(cfg.Options); err != nil {
		return nil, err
	}

	if c, err := container.GetContainer(cfg.Name, confBytes, storage); err == nil {
		return &hook{c}, nil
	} else {
		return nil, err
	}
}

// ErrTorrentUnapproved is the error returned when a torrent hash is invalid.
var ErrTorrentUnapproved = bittorrent.ClientError("unapproved torrent")

type hook struct {
	hashContainer container.Container
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (context.Context, error) {
	var err error

	if !h.hashContainer.Contains(req.InfoHash) {
		err = ErrTorrentUnapproved
	}

	return ctx, err
}

func (h *hook) HandleScrape(ctx context.Context, _ *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	// Scrapes don't require any protection.
	return ctx, nil
}

func (h *hook) Stop() stop.Result {
	if st, isOk := h.hashContainer.(stop.Stopper); isOk {
		return st.Stop()
	}
	return stop.AlreadyStopped
}
