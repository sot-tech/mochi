//go:build lmdb && cgo

package mdb

import (
	"context"
	"errors"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
	"os"
)

const (
	// Name - registered name of the storage
	Name           = "lmdb"
	defaultMode    = 0o640
	defaultMapSize = 1 << 28
)

var logger = log.NewLogger("storage/memory")

func init() {
	// Register the storage driver.
	storage.RegisterDriver(Name, builder{})
}

type builder struct{}

func (b builder) NewDataStorage(icfg conf.MapConfig) (storage.DataStorage, error) {
	return b.NewPeerStorage(icfg)
}

func (builder) NewPeerStorage(icfg conf.MapConfig) (storage.PeerStorage, error) {
	var cfg config
	if err := icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return newStorage(cfg)
}

type config struct {
	Path        string
	Mode        uint32
	DataDBName  string `cfg:"data_db"`
	PeersDBName string `cfg:"peers_db"`
	MaxSize     int64  `cfg:"max_size"`
}

var (
	errPathNotProvided  = errors.New("lmdb path not provided")
	errPathNotDirectory = errors.New("lmdb path is not directory")
)

func (cfg config) validate() (config, error) {
	validCfg := cfg
	if len(cfg.Path) == 0 {
		return cfg, errPathNotProvided
	} else {
		if stat, err := os.Stat(cfg.Path); err != nil {
			return cfg, err
		} else if !stat.IsDir() {
			return cfg, errPathNotDirectory
		}
	}
	if cfg.Mode == 0 {
		validCfg.Mode = defaultMode
		logger.Warn().
			Str("name", "mode").
			Stringer("provided", os.FileMode(cfg.Mode)).
			Stringer("default", os.FileMode(validCfg.Mode)).
			Msg("falling back to default configuration")
	}
	if cfg.MaxSize == 0 {
		validCfg.MaxSize = defaultMapSize
		logger.Warn().
			Str("name", "max_size").
			Int64("provided", cfg.MaxSize).
			Int64("default", validCfg.MaxSize).
			Msg("falling back to default configuration")
	}
	return validCfg, nil
}

type mdb struct {
	*lmdb.Env
	dataDB, peersDB lmdb.DBI
}

func newStorage(cfg config) (*mdb, error) {
	var err error
	if cfg, err = cfg.validate(); err != nil {
		return nil, err
	}
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	if err = env.SetMaxDBs(2); err != nil {
		return nil, err
	}
	if err = env.SetMapSize(cfg.MaxSize); err != nil {
		return nil, err
	}

	if err = env.Open(cfg.Path, 0, os.FileMode(cfg.Mode)); err != nil {
		return nil, err
	}

	var dataDB, peersDB lmdb.DBI
	if err = env.Update(func(txn *lmdb.Txn) (err error) {
		if len(cfg.DataDBName) > 0 {
			dataDB, err = txn.CreateDBI(cfg.DataDBName)
		} else {
			dataDB, err = txn.OpenRoot(0)
		}
		if err != nil {
			return
		}
		if len(cfg.PeersDBName) > 0 {
			peersDB, err = txn.CreateDBI(cfg.PeersDBName)
		} else {
			peersDB, err = txn.OpenRoot(0)
		}
		return
	}); err != nil {
		_ = env.Close()
		return nil, err
	}

	return &mdb{env, dataDB, peersDB}, nil
}

func (*mdb) Preservable() bool {
	return true
}

func (m *mdb) Close() (err error) {
	if m.Env != nil {
		err = m.Env.Close()
	}
	return
}

const keySeparator = '_'

func composeKey(ctx, key string) []byte {
	ctxLen := len(ctx)
	res := make([]byte, ctxLen+len(key)+1)
	copy(res, ctx)
	res[ctxLen] = keySeparator
	copy(res[ctxLen+1:], key)
	return res
}

func (m *mdb) Put(ctx context.Context, storeCtx string, values ...storage.Entry) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) Contains(ctx context.Context, storeCtx string, key string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func ignoreNotFound(data []byte, err error) ([]byte, error) {
	if err != nil && lmdb.IsNotFound(err) {
		err = nil
	}
	return data, err
}

func (m *mdb) Load(_ context.Context, storeCtx string, key string) (v []byte, err error) {
	err = m.Env.View(func(txn *lmdb.Txn) (err error) {
		v, err = ignoreNotFound(txn.Get(m.dataDB, composeKey(storeCtx, key)))
		return
	})
	return
}

func (m *mdb) Delete(ctx context.Context, storeCtx string, keys ...string) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) PutSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) DeleteSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) PutLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) DeleteLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) GraduateLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) AnnouncePeers(ctx context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) ScrapeSwarm(ctx context.Context, ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) Ping(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}
