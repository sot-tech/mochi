// Package torrentapproval implements a Hook that fails an Announce based on a
// whitelist or blacklist of torrent hash.
package torrentapproval

import (
	"context"
	"fmt"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/storage/memory"

	// import directory watcher to enable appropriate support
	_ "github.com/sot-tech/mochi/middleware/torrentapproval/container/directory"

	// import static list to enable appropriate support
	_ "github.com/sot-tech/mochi/middleware/torrentapproval/container/list"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/storage"
)

// Name is the name by which this middleware is registered with Conf.
const Name = "torrent approval"

func init() {
	middleware.RegisterBuilder(Name, build)
}

type baseConfig struct {
	// Source - name of container for initial values
	Source string `cfg:"initial_source"`
	// Preserve - if true, container will receive real registered storage if it is NOT `memory`
	// if false - temporary in-memory storage will be used or created
	Preserve bool
	// Configuration depends on used container
	Configuration conf.MapConfig
}

func build(config conf.MapConfig, st storage.PeerStorage) (h middleware.Hook, err error) {
	var cfg baseConfig
	if err = config.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("middleware %s: %w", Name, err)
	}

	if len(cfg.Source) == 0 {
		return nil, fmt.Errorf("invalid config for middleware %s: source not provided", Name)
	}

	if cfg.Configuration == nil {
		return nil, fmt.Errorf("invalid config for middleware %s: config not provided", Name)
	}

	var ds storage.DataStorage = st
	if !cfg.Preserve && ds.Preservable() {
		ds = memory.NewDataStorage()
	}

	var c container.Container
	if c, err = container.GetContainer(cfg.Source, cfg.Configuration, ds); err == nil {
		h = &hook{c}
	}
	return h, err
}

// ErrTorrentUnapproved is the error returned when a torrent hash is invalid.
var ErrTorrentUnapproved = bittorrent.ClientError("unapproved torrent")

type hook struct {
	hashContainer container.Container
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (context.Context, error) {
	var err error

	if !h.hashContainer.Approved(req.InfoHash) {
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
