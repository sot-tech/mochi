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
	var cfg config
	if err := icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return newStorage(cfg)
}

func (builder) NewPeerStorage(_ conf.MapConfig) (storage.PeerStorage, error) {
	panic("lmdb peer storage not implemented")
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
			peersDB, err = txn.OpenDBI(cfg.PeersDBName, lmdb.Create|lmdb.DupSort|lmdb.DupFixed)

		} else {
			peersDB, err = txn.OpenRoot(lmdb.DupSort | lmdb.DupFixed)
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

func (m *mdb) Put(_ context.Context, storeCtx string, values ...storage.Entry) (err error) {
	if len(values) > 0 {
		err = m.Update(func(txn *lmdb.Txn) (err error) {
			for _, kv := range values {
				if err = txn.Put(m.dataDB, composeKey(storeCtx, kv.Key), kv.Value, 0); err != nil {
					break
				}
			}
			return
		})
	}
	return
}

func (m *mdb) Contains(_ context.Context, storeCtx string, key string) (contains bool, err error) {
	err = m.View(func(txn *lmdb.Txn) (err error) {
		_, err = txn.Get(m.dataDB, composeKey(storeCtx, key))
		return
	})
	if err == nil {
		contains = true
	} else if lmdb.IsNotFound(err) {
		err = nil
	}
	return
}

func ignoreNotFound(data []byte, err error) ([]byte, error) {
	if err != nil && lmdb.IsNotFound(err) {
		err = nil
	}
	return data, err
}

func (m *mdb) Load(_ context.Context, storeCtx string, key string) (v []byte, err error) {
	err = m.View(func(txn *lmdb.Txn) (err error) {
		v, err = ignoreNotFound(txn.Get(m.dataDB, composeKey(storeCtx, key)))
		return
	})
	return
}

func (m *mdb) Delete(_ context.Context, storeCtx string, keys ...string) (err error) {
	if len(keys) > 0 {
		err = m.Update(func(txn *lmdb.Txn) (err error) {
			for _, k := range keys {
				if err = txn.Del(m.dataDB, composeKey(storeCtx, k), nil); err != nil {
					break
				}
			}
			return
		})
	}
	return
}

func (m *mdb) PutSeeder(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) DeleteSeeder(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) PutLeecher(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) DeleteLeecher(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) GraduateLeecher(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) AnnouncePeers(_ context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) ScrapeSwarm(_ context.Context, ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) Ping(_ context.Context) error {
	//TODO implement me
	panic("implement me")
}
