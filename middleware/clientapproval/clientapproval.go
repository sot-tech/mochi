// Package clientapproval implements a Hook that fails an Announce based on a
// whitelist or blacklist of BitTorrent client IDs.
package clientapproval

import (
	"context"
	"errors"
	"fmt"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/storage"
)

// Name is the name by which this middleware is registered with Conf.
const Name = "client approval"

func init() {
	middleware.RegisterBuilder(Name, build)
}

// ErrClientUnapproved is the error returned when a client's PeerID is invalid.
var ErrClientUnapproved = bittorrent.ClientError("client not allowed by mochi")

// Config represents all the values required by this middleware to validate
// peers based on their BitTorrent client ID.
type Config struct {
	// Static list of client IDs.
	ClientIDList []string `cfg:"client_id_list"`
	// If Invert set to true, all client IDs stored in ClientIDList should be blacklisted.
	Invert bool
}

type hook struct {
	clientIDs map[ClientID]any
	invert    bool
}

func build(config conf.MapConfig, _ storage.PeerStorage) (middleware.Hook, error) {
	var cfg Config

	if err := config.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("middleware %s: %w", Name, err)
	}

	h := &hook{
		clientIDs: make(map[ClientID]any, len(cfg.ClientIDList)),
		invert:    cfg.Invert,
	}

	for _, cidString := range cfg.ClientIDList {
		cidBytes := []byte(cidString)
		if len(cidBytes) != 6 {
			return nil, errors.New("client ID " + cidString + " must be 6 bytes")
		}
		h.clientIDs[ClientID(cidBytes)] = true
	}

	return h, nil
}

// HandleAnnounce checks if specified ClientID is approved or not.
// If Config.Invert set to true and hash found in provided list, function will return ErrClientUnapproved,
// that means that ClientID is blacklisted.
func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (context.Context, error) {
	var err error
	if _, contains := h.clientIDs[NewClientID(req.ID)]; contains == h.invert {
		err = ErrClientUnapproved
	}

	return ctx, err
}

func (h *hook) HandleScrape(ctx context.Context, _ *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	// Scrapes don't require any protection.
	return ctx, nil
}
