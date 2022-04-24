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

	"github.com/go-redis/redis/v8"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/storage"
	r "github.com/sot-tech/mochi/storage/redis"
)

// Name is name of this storage
const (
	Name            = "keydb"
	expireMemberCmd = "EXPIREMEMBER"
)

// ErrNotKeyDB returned from initializer if connected does not support KeyDB
// specific command (EXPIREMEMBER)
var ErrNotKeyDB = errors.New("provided instance seems not KeyDB")

func init() {
	// Register the storage driver.
	storage.RegisterBuilder(Name, builder)
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

	var rs r.Connection

	if rs, err = cfg.Connect(); err != nil {
		return nil, err
	}

	cmd := redis.NewCommandsInfoCmd(context.Background(), "COMMAND", "INFO", expireMemberCmd)
	_ = rs.Process(context.Background(), cmd)
	err = r.AsNil(cmd.Err())
	if err == nil && len(cmd.Val()) == 0 {
		err = ErrNotKeyDB
	}

	var st *store
	if err == nil {
		st = &store{
			Connection: rs,
			logFields:  cfg.LogFields(),
			peerTTL:    uint(cfg.PeerLifetime.Seconds()),
		}
	}

	return st, err
}

type store struct {
	r.Connection
	logFields log.Fields
	peerTTL   uint
}

func (s store) setPeerTTL(infoHashKey, peerID string) error {
	return s.Process(context.TODO(), redis.NewCmd(context.TODO(), expireMemberCmd, infoHashKey, peerID, s.peerTTL))
}

func (s store) addPeer(infoHashKey, peerID string) (err error) {
	log.Debug("storage: KeyDB: PutPeer", log.Fields{
		"infoHashKey": infoHashKey,
		"peerID":      peerID,
	})
	if err = s.SAdd(context.TODO(), infoHashKey, peerID).Err(); err == nil {
		err = s.setPeerTTL(infoHashKey, peerID)
	}
	return
}

func (s store) delPeer(infoHashKey, peerID string) error {
	log.Debug("storage: KeyDB: DeletePeer", log.Fields{
		"infoHashKey": infoHashKey,
		"peerID":      peerID,
	})
	deleted, err := s.SRem(context.TODO(), infoHashKey, peerID).Uint64()
	err = r.AsNil(err)
	if err == nil && deleted == 0 {
		err = storage.ErrResourceDoesNotExist
	}

	return err
}

func (s store) PutSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.addPeer(r.InfoHashKey(ih.RawString(), true, peer.Addr().Is6()), peer.RawString())
}

func (s store) DeleteSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(r.InfoHashKey(ih.RawString(), true, peer.Addr().Is6()), peer.RawString())
}

func (s store) PutLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.addPeer(r.InfoHashKey(ih.RawString(), false, peer.Addr().Is6()), peer.RawString())
}

func (s store) DeleteLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(r.InfoHashKey(ih.RawString(), false, peer.Addr().Is6()), peer.RawString())
}

func (s store) GraduateLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) (err error) {
	log.Debug("storage: KeyDB: GraduateLeecher", log.Fields{
		"infoHash": ih,
		"peer":     peer,
	})
	infoHash, peerID := ih.RawString(), peer.RawString()
	ihSeederKey := r.InfoHashKey(infoHash, true, peer.Addr().Is6())
	ihLeecherKey := r.InfoHashKey(infoHash, false, peer.Addr().Is6())
	var moved bool
	if moved, err = s.SMove(context.TODO(), ihLeecherKey, ihSeederKey, peerID).Result(); err == nil {
		if moved {
			err = s.setPeerTTL(ihSeederKey, peerID)
		} else {
			err = s.addPeer(ihSeederKey, peerID)
		}
	}
	return err
}

// AnnouncePeers is the same function as redis.AnnouncePeers
func (s store) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, peer bittorrent.Peer) ([]bittorrent.Peer, error) {
	log.Debug("storage: KeyDB: AnnouncePeers", log.Fields{
		"infoHash": ih,
		"seeder":   seeder,
		"numWant":  numWant,
		"peer":     peer,
	})

	return s.GetPeers(ih, seeder, numWant, peer, func(ctx context.Context, infoHashKey string, maxCount int) *redis.StringSliceCmd {
		return s.SRandMemberN(context.TODO(), infoHashKey, int64(maxCount))
	})
}

// ScrapeSwarm is the same function as redis.ScrapeSwarm except `SCard` call instead of `HLen`
func (s store) ScrapeSwarm(ih bittorrent.InfoHash, peer bittorrent.Peer) (leechers uint32, seeders uint32, snatched uint32) {
	log.Debug("storage: KeyDB ScrapeSwarm", log.Fields{
		"infoHash": ih,
		"peer":     peer,
	})
	leechers, seeders = s.CountPeers(ih, s.SCard)
	return
}

func (s *store) Stop() stop.Result {
	c := make(stop.Channel)
	if s.UniversalClient != nil {
		c.Done(s.UniversalClient.Close())
		s.UniversalClient = nil
	}
	return c.Result()
}

func (s store) LogFields() log.Fields {
	return s.logFields
}
