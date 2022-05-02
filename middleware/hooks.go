package middleware

import (
	"context"
	"errors"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/storage"
)

// Hook abstracts the concept of anything that needs to interact with a
// BitTorrent client's request and response to a BitTorrent tracker.
// PreHooks and PostHooks both use the same interface.
//
// A Hook can implement stop.Stopper if clean shutdown is required.
type Hook interface {
	HandleAnnounce(context.Context, *bittorrent.AnnounceRequest, *bittorrent.AnnounceResponse) (context.Context, error)
	HandleScrape(context.Context, *bittorrent.ScrapeRequest, *bittorrent.ScrapeResponse) (context.Context, error)
}

type skipSwarmInteraction struct{}

// SkipSwarmInteractionKey is a key for the context of an Announce to control
// whether the swarm interaction middleware should run.
// Any non-nil value set for this key will cause the swarm interaction
// middleware to skip.
var SkipSwarmInteractionKey = skipSwarmInteraction{}

type swarmInteractionHook struct {
	store storage.PeerStorage
}

func (h *swarmInteractionHook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (outCtx context.Context, err error) {
	outCtx = ctx
	if ctx.Value(SkipSwarmInteractionKey) != nil {
		return
	}

	var storeFn func(bittorrent.InfoHash, bittorrent.Peer) error

	switch {
	case req.Event == bittorrent.Stopped:
		storeFn = func(hash bittorrent.InfoHash, peer bittorrent.Peer) error {
			err = h.store.DeleteSeeder(hash, peer)
			if err != nil && !errors.Is(err, storage.ErrResourceDoesNotExist) {
				return err
			}

			err = h.store.DeleteLeecher(hash, peer)
			if err != nil && !errors.Is(err, storage.ErrResourceDoesNotExist) {
				return err
			}
			return nil
		}
	case req.Event == bittorrent.Completed:
		storeFn = h.store.GraduateLeecher
	case req.Left == 0:
		// Completed events will also have Left == 0, but by making this
		// an extra case we can treat "old" seeders differently from
		// graduating leechers. (Calling PutSeeder is probably faster
		// than calling GraduateLeecher.)
		storeFn = h.store.PutSeeder
	default:
		storeFn = h.store.PutLeecher
	}
	for _, p := range req.Peers() {
		if err = storeFn(req.InfoHash, p); err == nil && len(req.InfoHash) == bittorrent.InfoHashV2Len {
			err = storeFn(req.InfoHash.TruncateV1(), p)
		}
		if err != nil {
			break
		}
	}

	return
}

func (h *swarmInteractionHook) HandleScrape(ctx context.Context, _ *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	// Scrapes have no effect on the swarm.
	return ctx, nil
}

type skipResponseHook struct{}

// SkipResponseHookKey is a key for the context of an Announce or Scrape to
// control whether the response middleware should run.
// Any non-nil value set for this key will cause the response middleware to
// skip.
var SkipResponseHookKey = skipResponseHook{}

type responseHook struct {
	store storage.PeerStorage
}

func (h *responseHook) scrape(ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32) {
	leechers, seeders, snatched = h.store.ScrapeSwarm(ih)
	if len(ih) == bittorrent.InfoHashV2Len {
		l, s, n := h.store.ScrapeSwarm(ih.TruncateV1())
		leechers, seeders, snatched = leechers+l, seeders+s, snatched+n
	}
	return
}

func (h *responseHook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, resp *bittorrent.AnnounceResponse) (_ context.Context, err error) {
	if ctx.Value(SkipResponseHookKey) != nil {
		return ctx, nil
	}

	// Add the Scrape data to the response.
	resp.Incomplete, resp.Complete, _ = h.scrape(req.InfoHash)

	err = h.appendPeers(req, resp)
	return ctx, err
}

type fetchArgs struct {
	ih bittorrent.InfoHash
	v6 bool
}

func (h *responseHook) appendPeers(req *bittorrent.AnnounceRequest, resp *bittorrent.AnnounceResponse) (err error) {
	seeding := req.Left == 0
	max := int(req.NumWant)
	peers := make([]bittorrent.Peer, 0, len(resp.IPv4Peers)+len(resp.IPv6Peers))
	primaryIP := req.GetFirst()
	v6First := primaryIP.Is6()
	args := []fetchArgs{{req.InfoHash, v6First}, {req.InfoHash, !v6First}}

	if len(req.InfoHash) == bittorrent.InfoHashV2Len {
		ih := req.InfoHash.TruncateV1()
		args = append(args, fetchArgs{ih, v6First}, fetchArgs{ih, !v6First})
	}

	if v6First {
		peers = append(peers, resp.IPv6Peers...)
		peers = append(peers, resp.IPv4Peers...)
	} else {
		peers = append(peers, resp.IPv4Peers...)
		peers = append(peers, resp.IPv6Peers...)
	}
	if l := len(peers); l > max {
		peers, max = peers[:max], 0
	} else {
		max -= l
	}

	for _, a := range args {
		if max <= 0 {
			break
		}
		var storePeers []bittorrent.Peer
		storePeers, err = h.store.AnnouncePeers(a.ih, seeding, max, a.v6)
		if err != nil && !errors.Is(err, storage.ErrResourceDoesNotExist) {
			return err
		}
		err = nil
		peers = append(peers, storePeers...)
		max -= len(storePeers)
	}

	// Some clients expect a minimum of their own peer representation returned to
	// them if they are the only peer in a swarm.
	if len(peers) == 0 {
		if seeding {
			resp.Complete++
		} else {
			resp.Incomplete++
		}
		peers = append(peers, req.Peers()...)
	}

	l := len(peers)
	uniquePeers := make(map[bittorrent.Peer]interface{}, l)

	resp.IPv4Peers = make([]bittorrent.Peer, 0, l/2)
	resp.IPv6Peers = make([]bittorrent.Peer, 0, l/2)

	for _, p := range peers {
		if _, found := uniquePeers[p]; !found {
			if p.Addr().Is6() {
				resp.IPv6Peers = append(resp.IPv6Peers, p)
				uniquePeers[p] = nil
			} else if p.Addr().Is4() {
				resp.IPv4Peers = append(resp.IPv4Peers, p)
				uniquePeers[p] = nil
			} else {
				logger.Warn().Object("peer", p).Msg("received invalid peer from storage")
			}
		}
	}

	return
}

func (h *responseHook) HandleScrape(ctx context.Context, req *bittorrent.ScrapeRequest, resp *bittorrent.ScrapeResponse) (context.Context, error) {
	if ctx.Value(SkipResponseHookKey) != nil {
		return ctx, nil
	}

	for _, infoHash := range req.InfoHashes {
		scr := bittorrent.Scrape{InfoHash: infoHash}
		scr.Incomplete, scr.Complete, scr.Snatches = h.scrape(infoHash)
		resp.Files = append(resp.Files, scr)
	}

	return ctx, nil
}
