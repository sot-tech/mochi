//go:build cgo

package mdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/PowerDNS/lmdb-go/exp/lmdbsync"
	"net/netip"
	"os"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/PowerDNS/lmdb-go/lmdbscan"
	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

const (
	// Name - registered name of the storage
	Name              = "lmdb"
	defaultMode       = 0o640
	defaultMapSize    = 1 << 30
	defaultMaxReaders = 126
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
	// MaxSize - size of the memory map to use for lmdb environment.
	// The size should be a multiple of the OS page size.
	// Mochi's default is 1GiB.
	MaxSize int64 `cfg:"max_size"`
	// MaxReaders - maximum number of threads/reader slots for the LMDB environment.
	// LMDB library's default is 126.
	MaxReaders int `cfg:"max_readers"`
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
	if cfg.MaxSize <= 0 {
		validCfg.MaxSize = defaultMapSize
		logger.Warn().
			Str("name", "max_size").
			Int64("provided", cfg.MaxSize).
			Int64("default", validCfg.MaxSize).
			Msg("falling back to default configuration")
	}
	if cfg.MaxReaders <= 0 {
		validCfg.MaxReaders = defaultMaxReaders
		logger.Warn().
			Str("name", "max_readers").
			Int("provided", cfg.MaxReaders).
			Int("default", 126).
			Msg("falling back to default configuration")
	}
	return validCfg, nil
}

type mdb struct {
	*lmdbsync.Env
	dataDB, peersDB lmdb.DBI
}

func newStorage(cfg config) (*mdb, error) {
	var err error
	if cfg, err = cfg.validate(); err != nil {
		return nil, err
	}
	lmEnv, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	var env *lmdbsync.Env

	if env, err = lmdbsync.NewEnv(lmEnv,
		lmdbsync.MapResizedHandler(lmdbsync.MapResizedDefaultRetry, lmdbsync.MapResizedDefaultDelay),
	); err != nil {
		return nil, err
	}

	if err = env.SetMaxDBs(2); err != nil {
		return nil, err
	}
	if err = env.SetMapSize(cfg.MaxSize); err != nil {
		return nil, err
	}
	if cfg.MaxReaders > 0 {
		if err = env.SetMaxReaders(cfg.MaxReaders); err != nil {
			return nil, err
		}
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

func ignoreNotFound(err error) error {
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return err
}

func ignoreNotFoundData(data []byte, err error) ([]byte, error) {
	if lmdb.IsNotFound(err) {
		err = nil
	}
	return data, err
}

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
			var data []byte
			for _, kv := range values {
				vl := len(kv.Value)
				if data, err = txn.PutReserve(m.dataDB, composeKey(storeCtx, kv.Key), vl, 0); err == nil {
					copy(data, kv.Value)
				} else {
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

func (m *mdb) Load(_ context.Context, storeCtx string, key string) (v []byte, err error) {
	err = m.View(func(txn *lmdb.Txn) (err error) {
		v, err = ignoreNotFoundData(txn.Get(m.dataDB, composeKey(storeCtx, key)))
		return
	})
	return
}

func (m *mdb) Delete(_ context.Context, storeCtx string, keys ...string) (err error) {
	if len(keys) > 0 {
		err = m.Update(func(txn *lmdb.Txn) (err error) {
			for _, k := range keys {
				if err = ignoreNotFound(txn.Del(m.dataDB, composeKey(storeCtx, k), nil)); err != nil {
					break
				}
			}
			return
		})
	}
	return
}

const (
	ipLen            = 16
	packedPeerLen    = bittorrent.PeerIDLen + ipLen + 2 // peer_id + ipv6 + port
	seederPrefix     = 'S'
	leecherPrefix    = 'L'
	ipv4Prefix       = '4'
	ipv6Prefix       = '6'
	countPrefix      = 'C'
	downloadedPrefix = 'D'
)

func packPeer(peer bittorrent.Peer, out []byte) {
	_ = out[packedPeerLen-1]
	copy(out, peer.ID.Bytes())
	a := peer.Addr().As16()
	copy(out[bittorrent.PeerIDLen:], a[:])
	binary.BigEndian.PutUint16(out[bittorrent.PeerIDLen+ipLen:], peer.Port())
	return
}

func unpackPeer(arr []byte) (peer bittorrent.Peer) {
	_ = arr[packedPeerLen-1]
	peerID, _ := bittorrent.NewPeerID(arr[:bittorrent.PeerIDLen])
	peer = bittorrent.Peer{
		ID: peerID,
		AddrPort: netip.AddrPortFrom(netip.AddrFrom16([ipLen]byte(arr[bittorrent.PeerIDLen:])).Unmap(),
			binary.BigEndian.Uint16(arr[bittorrent.PeerIDLen+ipLen:])),
	}
	return
}

func composeIHKeyPrefix(ih bittorrent.InfoHash, seeder bool, v6 bool, suffixLen int) (ihKey []byte, suffixStart int) {
	ihLen := len(ih)
	ihKey = make([]byte, ihLen+4+suffixLen) // prefix{L/S} + prefix{4/6} + separator + infoHash + separator
	if seeder {
		ihKey[0] = seederPrefix
	} else {
		ihKey[0] = leecherPrefix
	}
	if v6 {
		ihKey[1] = ipv6Prefix
	} else {
		ihKey[1] = ipv4Prefix
	}
	ihKey[2], ihKey[ihLen+3] = keySeparator, keySeparator
	copy(ihKey[3:], ih)
	suffixStart = len(ihKey) - suffixLen
	return
}

func composeIHKey(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) (ihKey []byte) {
	ihKey, start := composeIHKeyPrefix(ih, seeder, peer.Addr().Is6(), packedPeerLen)
	packPeer(peer, ihKey[start:])
	return
}

func (m *mdb) incr(txn *lmdb.Txn, key []byte, inc int) (err error) {
	var v int
	var b []byte
	if b, err = ignoreNotFoundData(txn.Get(m.peersDB, key)); err != nil {
		return
	}
	if len(b) >= 4 {
		v = int(binary.BigEndian.Uint32(b))
	}
	v += inc
	if v < 0 {
		v = 0
	}
	if b, err = txn.PutReserve(m.peersDB, key, 4, 0); err == nil {
		binary.BigEndian.PutUint32(b, uint32(v))
	}
	return
}

func (m *mdb) putPeer(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) error {
	ihKey := composeIHKey(ih, peer, seeder)
	return m.Update(func(txn *lmdb.Txn) (err error) {
		var b []byte
		if b, err = txn.PutReserve(m.peersDB, ihKey, 8, 0); err == nil {
			binary.BigEndian.PutUint64(b, uint64(timecache.NowUnixNano()))
			ihKey[1] = countPrefix
			err = m.incr(txn, ihKey[:len(ihKey)-packedPeerLen], 1)
		}
		return
	})
}

func (m *mdb) delPeer(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) error {
	ihKey := composeIHKey(ih, peer, seeder)
	return m.Update(func(txn *lmdb.Txn) (err error) {
		if err = ignoreNotFound(txn.Del(m.peersDB, ihKey, nil)); err == nil {
			ihKey[1] = countPrefix
			err = m.incr(txn, ihKey[:len(ihKey)-packedPeerLen], -1)
		}
		return
	})
}

func (m *mdb) PutSeeder(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return m.putPeer(ih, peer, true)
}

func (m *mdb) DeleteSeeder(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return m.delPeer(ih, peer, true)
}

func (m *mdb) PutLeecher(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return m.putPeer(ih, peer, false)
}

func (m *mdb) DeleteLeecher(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return m.delPeer(ih, peer, false)
}

func (m *mdb) GraduateLeecher(_ context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	ihKey := composeIHKey(ih, peer, false)
	return m.Update(func(txn *lmdb.Txn) (err error) {
		if err = ignoreNotFound(txn.Del(m.peersDB, ihKey, nil)); err != nil {
			return
		}
		ihKey[0] = seederPrefix
		var b []byte
		if b, err = txn.PutReserve(m.peersDB, ihKey, 8, 0); err != nil {
			return
		}
		binary.BigEndian.PutUint64(b, uint64(timecache.NowUnixNano()))
		ihPrefix := ihKey[:len(ihKey)-packedPeerLen]
		ihPrefix[1] = countPrefix
		if err = m.incr(txn, ihPrefix, 1); err != nil {
			return
		}
		ihPrefix[0] = leecherPrefix
		if err = m.incr(txn, ihPrefix, -1); err != nil {
			return
		}
		ihPrefix[0] = downloadedPrefix
		err = m.incr(txn, ihPrefix, 1)
		return
	})
}

type scanAction int

const (
	next scanAction = iota
	stop
	del
)

func (m *mdb) scanPeers(ctx context.Context, prefix []byte, rw bool, fn func(k, v []byte) scanAction) (err error) {
	prefixLen := len(prefix)
	txFunc := func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true
		scanner := lmdbscan.New(txn, m.peersDB)
		defer scanner.Close()
		if scanner.SetNext(prefix, nil, lmdb.SetRange, lmdb.Next) {
		loop:
			for scanner.Scan() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					k := scanner.Key()
					if !bytes.HasPrefix(k, prefix) {
						break loop
					}
					if len(k) == prefixLen+packedPeerLen {
						switch fn(k, scanner.Val()) {
						case del:
							if err = scanner.Cursor().Del(0); err != nil {
								break loop
							}
						case stop:
							break loop
						}
					} else {
						logger.Warn().Int("expected", prefixLen+packedPeerLen).Int("got", len(k)).
							Msg("Invalid key length")
					}
				}
			}
			err = scanner.Err()
		}
		return
	}

	if rw {
		err = m.Update(txFunc)
	} else {
		err = m.View(txFunc)
	}

	return
}

func (m *mdb) AnnouncePeers(ctx context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error) {
	peers = make([]bittorrent.Peer, 0, numWant)
	prefix, prefixLen := composeIHKeyPrefix(ih, false, v6, 0)
	appendFn := func(k, _ []byte) scanAction {
		peers = append(peers, unpackPeer(k[prefixLen:]))
		numWant--
		res := next
		if numWant == 0 {
			res = stop
		}
		return res
	}
	if forSeeder {
		err = m.scanPeers(ctx, prefix, false, appendFn)
	} else {
		prefix[0] = seederPrefix
		if err = m.scanPeers(ctx, prefix, false, appendFn); err == nil && numWant > 0 {
			prefix[0] = leecherPrefix
			err = m.scanPeers(ctx, prefix, false, appendFn)
		}
	}
	return
}

func (m *mdb) ScrapeSwarm(_ context.Context, ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32, err error) {
	prefix, _ := composeIHKeyPrefix(ih, false, false, 0)
	prefix[1] = countPrefix
	var b []byte
	err = m.View(func(txn *lmdb.Txn) (err error) {
		if b, err = ignoreNotFoundData(txn.Get(m.peersDB, prefix)); err != nil {
			return
		} else if len(b) >= 4 {
			leechers = binary.BigEndian.Uint32(b)
		}

		prefix[0] = seederPrefix
		if b, err = ignoreNotFoundData(txn.Get(m.peersDB, prefix)); err != nil {
			return
		} else if len(b) >= 4 {
			seeders = binary.BigEndian.Uint32(b)
		}

		prefix[0] = downloadedPrefix
		if b, err = ignoreNotFoundData(txn.Get(m.peersDB, prefix)); err != nil {
			return
		} else if len(b) >= 4 {
			snatched = binary.BigEndian.Uint32(b)
		}
		return
	})
	return
}

func (m *mdb) ScheduleGC(gcInterval, peerLifeTime time.Duration) {
	//TODO implement me
	panic("implement me")
}

func (m *mdb) Ping(_ context.Context) error {
	_, err := m.Info()
	return err
}
