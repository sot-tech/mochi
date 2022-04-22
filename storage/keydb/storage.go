// Package keydb implements the storage interface.
// This storage mostly is the same as redis, but it collects peers
// not in hashes, but in sets and uses KeyDB-specific command
// `EXPIREMEMBER` and, so it does not need garbage collection.
// Note: this storage also does not support statistics collection
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
	storage.RegisterBuilder(Name, Builder)
}

func Builder(icfg conf.MapConfig) (storage.PeerStorage, error) {
	var cfg r.Config
	var err error

	if err = icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return New(cfg)
}

func New(cfg r.Config) (*store, error) {
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
		st = &store{rs, cfg.LogFields()}
	}

	return st, err
}

type store struct {
	r.Connection
	logFields log.Fields
}

func (s store) PutSeeder(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error {
	// TODO implement me
	panic("implement me")
}

func (s store) DeleteSeeder(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error {
	// TODO implement me
	panic("implement me")
}

func (s store) PutLeecher(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error {
	// TODO implement me
	panic("implement me")
}

func (s store) DeleteLeecher(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error {
	// TODO implement me
	panic("implement me")
}

func (s store) GraduateLeecher(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error {
	// TODO implement me
	panic("implement me")
}

func (s store) AnnouncePeers(infoHash bittorrent.InfoHash, seeder bool, numWant int, peer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	// TODO implement me
	panic("implement me")
}

func (s store) ScrapeSwarm(infoHash bittorrent.InfoHash, peer bittorrent.Peer) bittorrent.Scrape {
	// TODO implement me
	panic("implement me")
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
