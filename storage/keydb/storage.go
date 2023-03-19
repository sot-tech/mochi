// Package keydb implements the storage interface.
// This storage mostly is the same as redis, but
// uses KeyDB-specific command `EXPIREMEMBER`, so it
// does not need garbage collection.
//
// Storage uses redis.IHSeederKey and redis.IHLeecherKey,
// BUT they are NOT compatible with each other because of
// another structure (hash in redis and set in keydb).
// Note: this storage also does not support statistics collection.
package keydb

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
	r "github.com/sot-tech/mochi/storage/redis"
)

const expireMemberCmd = "EXPIREMEMBER"

var (
	logger = log.NewLogger("storage/keydb")
	// errNotKeyDB returned from initializer if connected does not support KeyDB
	// specific command (EXPIREMEMBER)
	errNotKeyDB = errors.New("provided instance seems not KeyDB")
)

func init() {
	// Register the storage driver.
	storage.RegisterDriver("keydb", builder)
}

func builder(icfg conf.MapConfig) (storage.PeerStorage, error) {
	var cfg r.Config
	var err error

	if err = icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return newStore(cfg)
}

func newStore(cfg r.Config) (*store, error) {
	var err error
	if cfg, err = cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.PeerLifetime <= 0 {
		logger.Warn().
			Str("name", "peerLifetime").
			Dur("provided", cfg.PeerLifetime).
			Dur("default", storage.DefaultPeerLifetime).
			Msg("falling back to default configuration")
		cfg.PeerLifetime = storage.DefaultPeerLifetime
	}

	var rs r.Connection

	if rs, err = cfg.Connect(); err != nil {
		return nil, err
	}

	cmd := redis.NewCommandsInfoCmd(context.Background(), "COMMAND", "INFO", expireMemberCmd)
	_ = rs.Process(context.Background(), cmd)
	err = r.NoResultErr(cmd.Err())
	if err == nil && len(cmd.Val()) == 0 {
		err = errNotKeyDB
	}

	var st *store
	if err == nil {
		st = &store{Connection: rs, peerTTL: uint(cfg.PeerLifetime.Seconds())}
	}

	return st, err
}

type store struct {
	r.Connection
	peerTTL uint
}

func (s *store) addPeer(ctx context.Context, infoHashKey, peerID string) (err error) {
	logger.Trace().
		Str("infoHashKey", infoHashKey).
		Str("peerID", peerID).
		Msg("add peer")
	if err = s.SAdd(ctx, infoHashKey, peerID).Err(); err == nil {
		err = s.Process(ctx, redis.NewCmd(ctx, expireMemberCmd, infoHashKey, peerID, s.peerTTL))
	}
	return
}

func (s *store) delPeer(ctx context.Context, infoHashKey, peerID string) error {
	logger.Trace().
		Str("infoHashKey", infoHashKey).
		Str("peerID", peerID).
		Msg("del peer")
	deleted, err := s.SRem(ctx, infoHashKey, peerID).Uint64()
	err = r.NoResultErr(err)
	if err == nil && deleted == 0 {
		err = storage.ErrResourceDoesNotExist
	}

	return err
}

func (s *store) PutSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.addPeer(ctx, r.InfoHashKey(ih.RawString(), true, peer.Addr().Is6()), peer.RawString())
}

func (s *store) DeleteSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(ctx, r.InfoHashKey(ih.RawString(), true, peer.Addr().Is6()), peer.RawString())
}

func (s *store) PutLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.addPeer(ctx, r.InfoHashKey(ih.RawString(), false, peer.Addr().Is6()), peer.RawString())
}

func (s *store) DeleteLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(ctx, r.InfoHashKey(ih.RawString(), false, peer.Addr().Is6()), peer.RawString())
}

func (s *store) GraduateLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) (err error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Msg("graduate leecher")
	infoHash, peerID := ih.RawString(), peer.RawString()
	ihSeederKey := r.InfoHashKey(infoHash, true, peer.Addr().Is6())
	ihLeecherKey := r.InfoHashKey(infoHash, false, peer.Addr().Is6())
	var moved bool
	if moved, err = s.SMove(ctx, ihLeecherKey, ihSeederKey, peerID).Result(); err == nil {
		if moved {
			err = s.Process(ctx, redis.NewCmd(ctx, expireMemberCmd, ihSeederKey, peerID, s.peerTTL))
		} else {
			err = s.addPeer(ctx, ihSeederKey, peerID)
		}
		if err == nil {
			err = s.HIncrBy(ctx, r.CountDownloadsKey, infoHash, 1).Err()
		}
	}
	return err
}

// AnnouncePeers is the same function as redis.AnnouncePeers
func (s *store) AnnouncePeers(ctx context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) ([]bittorrent.Peer, error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Bool("forSeeder", forSeeder).
		Int("numWant", numWant).
		Bool("v6", v6).
		Msg("announce peers")

	return s.GetPeers(ih, forSeeder, numWant, v6, func(infoHashKey string, maxCount int) *redis.StringSliceCmd {
		return s.SRandMemberN(ctx, infoHashKey, int64(maxCount))
	})
}

// ScrapeSwarm is the same function as redis.ScrapeSwarm except `SCard` call instead of `HLen`
func (s *store) ScrapeSwarm(ctx context.Context, ih bittorrent.InfoHash) (uint32, uint32, uint32, error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Msg("scrape swarm")
	return s.ScrapeIH(ctx, ih, s.SCard)
}
