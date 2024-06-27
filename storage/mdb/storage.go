//go:build cgo

// Package mdb implements LMDB data and peer storage
package mdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/PowerDNS/lmdb-go/exp/lmdbsync"
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

var logger = log.NewLogger("storage/lmdb")

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
	// AsyncWrite sets MDB_WRITEMAP and MDB_MAPASYNC flags to use asynchronous flushes to disk.
	AsyncWrite bool `cfg:"async_write"`
	// NoMetaSync sets MDB_NOMETASYNC flag, omit the metadata flush.
	NoMetaSync bool `cfg:"no_sync_meta"`
}

var (
	errPathNotProvided  = errors.New("lmdb path not provided")
	errPathNotDirectory = errors.New("lmdb path is not directory")
)

func (cfg config) validate() (config, error) {
	validCfg := cfg
	if len(cfg.Path) == 0 {
		return cfg, errPathNotProvided
	} else if stat, err := os.Stat(cfg.Path); err != nil {
		return cfg, err
	} else if !stat.IsDir() {
		return cfg, errPathNotDirectory
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

type lmdbEnv interface {
	SetMaxDBs(int) error
	SetMapSize(int64) error
	SetMaxReaders(int) error
	Open(string, uint, os.FileMode) error
	View(lmdb.TxnOp) error
	Update(lmdb.TxnOp) error
	Close() error
	Sync(bool) error
	Info() (*lmdb.EnvInfo, error)
}

type mdb struct {
	lmdbEnv
	dataDB, peersDB lmdb.DBI
	onceCloser      sync.Once
	closed          chan any
	wg              sync.WaitGroup
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
	var env lmdbEnv

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

	var flags uint
	if cfg.AsyncWrite {
		flags |= lmdb.WriteMap | lmdb.MapAsync
	}
	if cfg.NoMetaSync {
		flags |= lmdb.NoMetaSync
	}

	if err = env.Open(cfg.Path, flags, os.FileMode(cfg.Mode)); err != nil {
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

	return &mdb{
		lmdbEnv: env,
		dataDB:  dataDB,
		peersDB: peersDB,
		closed:  make(chan any),
	}, nil
}

func (*mdb) Preservable() bool {
	return true
}

func (m *mdb) Close() (err error) {
	m.onceCloser.Do(func() {
		if m.lmdbEnv != nil {
			close(m.closed)
			m.wg.Wait()
			logger.Info().Msg("LMDB exiting. Flushing databases to disk")
			_ = m.Sync(true)
			err = m.lmdbEnv.Close()
		}
	})
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

func composeIHKeyPrefix(ih []byte, seeder bool, v6 bool, suffixLen int) (ihKey []byte, suffixStart int) {
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
	ihKey, start := composeIHKeyPrefix(ih.Bytes(), seeder, peer.Addr().Is6(), packedPeerLen)
	packPeer(peer, ihKey[start:])
	return
}

func (m *mdb) putPeer(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) error {
	ihKey := composeIHKey(ih, peer, seeder)
	return m.Update(func(txn *lmdb.Txn) (err error) {
		var b []byte
		if b, err = txn.PutReserve(m.peersDB, ihKey, 8, 0); err == nil {
			binary.BigEndian.PutUint64(b, uint64(timecache.NowUnix()))
		}
		return
	})
}

func (m *mdb) delPeer(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) error {
	ihKey := composeIHKey(ih, peer, seeder)
	return m.Update(func(txn *lmdb.Txn) error {
		return ignoreNotFound(txn.Del(m.peersDB, ihKey, nil))
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
		ihPrefix[0], ihPrefix[1] = downloadedPrefix, countPrefix
		var v int
		if b, err = ignoreNotFoundData(txn.Get(m.peersDB, ihPrefix)); err != nil {
			return
		}
		if len(b) >= 4 {
			v = int(binary.BigEndian.Uint32(b))
		}
		v++
		if b, err = txn.PutReserve(m.peersDB, ihPrefix, 4, 0); err == nil {
			binary.BigEndian.PutUint32(b, uint32(v))
		}
		return
	})
}

func (m *mdb) scanPeers(ctx context.Context, prefix []byte, readRaw bool, fn func(k, v []byte) bool) (err error) {
	m.wg.Add(1)
	prefixLen := len(prefix)
	err = m.View(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = readRaw
		scanner := lmdbscan.New(txn, m.peersDB)
		var op uint = lmdb.SetRange
		if prefixLen == 0 {
			op = lmdb.First
		}
		if scanner.SetNext(prefix, nil, op, lmdb.Next) {
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
					if prefixLen == 0 || len(k) == prefixLen+packedPeerLen {
						if !fn(k, scanner.Val()) {
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
		scanner.Close()
		return
	})
	m.wg.Done()

	return
}

func (m *mdb) AnnouncePeers(ctx context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error) {
	peers = make([]bittorrent.Peer, 0, numWant)
	prefix, prefixLen := composeIHKeyPrefix(ih.Bytes(), false, v6, 0)
	appendFn := func(k, _ []byte) bool {
		peers = append(peers, unpackPeer(k[prefixLen:]))
		numWant--
		return numWant > 0
	}
	if forSeeder {
		err = m.scanPeers(ctx, prefix, true, appendFn)
	} else {
		prefix[0] = seederPrefix
		if err = m.scanPeers(ctx, prefix, true, appendFn); err == nil && numWant > 0 {
			prefix[0] = leecherPrefix
			err = m.scanPeers(ctx, prefix, true, appendFn)
		}
	}
	return
}

func (m *mdb) countPeers(ctx context.Context, scanPrefix []byte) (cnt uint32, err error) {
	m.wg.Add(1)
	err = m.View(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true
		scanner := lmdbscan.New(txn, m.peersDB)
		if scanner.SetNext(scanPrefix, nil, lmdb.SetRange, lmdb.Next) {
			var prevKey []byte
		loop:
			for scanner.Scan() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					k := scanner.Key()
					if len(k) == len(scanPrefix)+packedPeerLen && bytes.HasPrefix(scanPrefix, k[:len(k)-packedPeerLen]) {
						if !bytes.Equal(k, prevKey) {
							cnt++
							prevKey = k
						}
					} else if scanPrefix[1] == ipv4Prefix {
						scanPrefix[1] = ipv6Prefix
						if !scanner.SetNext(scanPrefix, nil, lmdb.SetRange, lmdb.Next) {
							break loop
						}
					} else {
						break loop
					}
				}
			}
		}
		err = scanner.Err()
		scanner.Close()
		return
	})
	m.wg.Done()

	return
}

func (m *mdb) ScrapeSwarm(ctx context.Context, ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32, err error) {
	scanPrefix, _ := composeIHKeyPrefix(ih.Bytes(), false, false, 0)
	if leechers, err = m.countPeers(ctx, scanPrefix); err != nil {
		return
	}
	scanPrefix[0], scanPrefix[1] = seederPrefix, ipv4Prefix
	if seeders, err = m.countPeers(ctx, scanPrefix); err != nil {
		return
	}

	scanPrefix[0], scanPrefix[1] = downloadedPrefix, countPrefix
	err = m.View(func(txn *lmdb.Txn) (err error) {
		var b []byte
		if b, err = ignoreNotFoundData(txn.Get(m.peersDB, scanPrefix)); err != nil {
			return
		}
		if len(b) >= 4 {
			snatched = binary.BigEndian.Uint32(b)
		}
		return
	})
	return
}

const (
	v1IHKeyLen = bittorrent.InfoHashV1Len + 4 + packedPeerLen
	v2IHKeyPen = bittorrent.InfoHashV2Len + 4 + packedPeerLen
)

func (m *mdb) gc(cutoff time.Time) {
	toDel := make([][]byte, 0, 50)
	cutoffUnix := cutoff.Unix()
	err := m.scanPeers(context.Background(), nil, false, func(k, v []byte) bool {
		if l := len(k); (l == v1IHKeyLen || l == v2IHKeyPen) &&
			(k[0] == seederPrefix || k[0] == leecherPrefix) &&
			(k[1] == ipv4Prefix || k[1] == ipv6Prefix) &&
			k[2] == keySeparator && len(v) >= 8 && cutoffUnix >= int64(binary.BigEndian.Uint64(v)) {
			toDel = append(toDel, k)
		}
		return true
	})
	if err == nil {
		err = m.Update(func(txn *lmdb.Txn) (err error) {
			for _, k := range toDel {
				if err = txn.Del(m.peersDB, k, nil); err != nil {
					break
				}
			}
			return
		})
	}
	if err == nil {
		_ = m.Sync(true)
	} else {
		logger.Err(err).Msg("Error occurred while GC")
	}
}

func (m *mdb) ScheduleGC(gcInterval, peerLifeTime time.Duration) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		t := time.NewTimer(gcInterval)
		defer t.Stop()
		for {
			select {
			case <-m.closed:
				return
			case <-t.C:
				m.gc(time.Now().Add(-peerLifeTime))
				t.Reset(gcInterval)
			}
		}
	}()
}

func (m *mdb) Ping(_ context.Context) error {
	_, err := m.Info()
	return err
}
