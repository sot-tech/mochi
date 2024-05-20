// Package torrentapproval implements a Hook that fails an Announce based on a
// whitelist or blacklist of torrent hash.
package torrentapproval

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container"
	"github.com/sot-tech/mochi/pkg/conf"
	// import directory watcher to enable appropriate support
	_ "github.com/sot-tech/mochi/middleware/torrentapproval/container/directory"

	// import static list to enable appropriate support
	_ "github.com/sot-tech/mochi/middleware/torrentapproval/container/list"
	"github.com/sot-tech/mochi/storage"
)

// Name is the name by which this middleware is registered with Conf.
const Name = "torrent approval"

const internalStore = "internal"

func init() {
	middleware.RegisterBuilder(Name, build)
}

type baseConfig struct {
	// Source - name of container for initial values
	Source string `cfg:"initial_source"`
	// Deprecated: use Store parameter
	Preserve bool
	// Store where to hold provided data by Source
	Store conf.NamedMapConfig
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

	if cfg.Preserve {
		return nil, errors.New("preserve option is deprecated, use store parameter")
	}

	var ds storage.DataStorage
	if len(cfg.Store.Name) == 0 || cfg.Store.Name == internalStore {
		ds = st
	} else if ds, err = storage.NewDataStorage(cfg.Store); err != nil {
		return
	}

	var c container.Container
	if c, err = container.GetContainer(cfg.Source, cfg.Configuration, ds); err == nil {
		h = &hook{c}
	}
	return h, err
}

// ErrTorrentUnapproved is the error returned when a torrent hash is invalid.
var ErrTorrentUnapproved = bittorrent.ClientError("torrent not allowed by mochi")

type hook struct {
	hashContainer container.Container
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (context.Context, error) {
	var err error

	if !h.hashContainer.Approved(ctx, req.InfoHash) {
		err = ErrTorrentUnapproved
	}

	return ctx, err
}

func (h *hook) HandleScrape(ctx context.Context, _ *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	// Scrapes don't require any protection.
	return ctx, nil
}

func (h *hook) Close() (err error) {
	if cl, isOk := h.hashContainer.(io.Closer); isOk {
		err = cl.Close()
	}
	return err
}
