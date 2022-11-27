// Package memory implements the storage interface for a Conf
// BitTorrent tracker keeping peer data in memory.
package memory

import (
	"context"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cornelk/hashmap"
	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

const (
	// Name - registered name of the storage
	Name = "memory"
	// Default config constants.
	defaultShardCount = 1024
	// -1
	decrUint64 = ^uint64(0)
)

var logger = log.NewLogger("storage/memory")

func init() {
	// Register the storage driver.
	storage.RegisterDriver(Name, builder)
}

func builder(icfg conf.MapConfig) (storage.PeerStorage, error) {
	var cfg Config
	if err := icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return NewPeerStorage(cfg)
}

// Config holds the configuration of a memory PeerStorage.
type Config struct {
	ShardCount int `cfg:"shard_count"`
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() Config {
	validcfg := cfg

	if cfg.ShardCount <= 0 || cfg.ShardCount > (math.MaxInt/2) {
		validcfg.ShardCount = defaultShardCount
		log.Warn().
			Str("name", "ShardCount").
			Int("provided", cfg.ShardCount).
			Int("default", validcfg.ShardCount).
			Msg("falling back to default configuration")
	}

	return validcfg
}

// NewPeerStorage creates a new PeerStorage backed by memory.
func NewPeerStorage(provided Config) (storage.PeerStorage, error) {
	cfg := provided.Validate()
	ps := &peerStore{
		shards:      make([]*peerShard, cfg.ShardCount*2),
		DataStorage: NewDataStorage(),
		closed:      make(chan any),
	}

	for i := 0; i < cfg.ShardCount*2; i++ {
		ps.shards[i] = &peerShard{swarms: hashmap.New[bittorrent.InfoHash, swarm]()}
	}

	return ps, nil
}

type peerShard struct {
	swarms      *hashmap.Map[bittorrent.InfoHash, swarm]
	numSeeders  atomic.Uint64
	numLeechers atomic.Uint64
	sync.Mutex
}

// ginSwarm returns existing swarm or inserts new empty if it does not exist
func (sh *peerShard) ginSwarm(ih bittorrent.InfoHash) swarm {
	sw, ok := sh.swarms.Get(ih)
	if !ok {
		sh.Lock()
		defer sh.Unlock()
		sw, _ = sh.swarms.GetOrInsert(ih, swarm{
			seeders:  hashmap.New[string, int64](),
			leechers: hashmap.New[string, int64](),
		})
	}
	return sw
}

type swarm struct {
	// map serialized peer to mtime
	seeders  *hashmap.Map[string, int64]
	leechers *hashmap.Map[string, int64]
}

type peerStore struct {
	storage.DataStorage
	shards []*peerShard

	closed     chan any
	wg         sync.WaitGroup
	onceCloser sync.Once
}

var _ storage.PeerStorage = &peerStore{}

func (ps *peerStore) ScheduleGC(gcInterval, peerLifeTime time.Duration) {
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		t := time.NewTimer(gcInterval)
		defer t.Stop()
		for {
			select {
			case <-ps.closed:
				return
			case <-t.C:
				before := time.Now().Add(-peerLifeTime)
				logger.Trace().Time("before", before).Msg("purging peers with no announces")
				start := time.Now()
				ps.gc(before)
				duration := time.Since(start)
				logger.Debug().Dur("timeTaken", duration).Msg("gc complete")
				storage.PromGCDurationMilliseconds.Observe(float64(duration.Milliseconds()))
			}
		}
	}()
}

func (ps *peerStore) ScheduleStatisticsCollection(reportInterval time.Duration) {
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		t := time.NewTicker(reportInterval)
		for {
			select {
			case <-ps.closed:
				t.Stop()
				return
			case <-t.C:
				if metrics.Enabled() {
					before := time.Now()
					// aggregates metrics over all shards and then posts them to
					// prometheus.
					var numInfoHashes, numSeeders, numLeechers uint64

					for _, s := range ps.shards {
						numInfoHashes += uint64(s.swarms.Len())
						numSeeders += s.numSeeders.Load()
						numLeechers += s.numLeechers.Load()
					}

					storage.PromInfoHashesCount.Set(float64(numInfoHashes))
					storage.PromSeedersCount.Set(float64(numSeeders))
					storage.PromLeechersCount.Set(float64(numLeechers))
					logger.Debug().TimeDiff("timeTaken", time.Now(), before).Msg("populate prom complete")
				}
			}
		}
	}()
}

func (ps *peerStore) shardIndex(infoHash bittorrent.InfoHash, v6 bool) uint32 {
	// There are twice the amount of shards specified by the user, the first
	// half is dedicated to IPv4 swarms and the second half is dedicated to
	// IPv6 swarms.
	idx := binary.BigEndian.Uint32([]byte(infoHash[:4])) % (uint32(len(ps.shards)) / 2)
	if v6 {
		idx += uint32(len(ps.shards) / 2)
	}
	return idx
}

func (ps *peerStore) PutSeeder(_ context.Context, ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", p).
		Msg("put seeder")

	sh := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	sw, pID := sh.ginSwarm(ih), p.RawString()

	if _, exists := sw.seeders.Get(pID); !exists {
		sh.numSeeders.Add(1)
	}

	sw.seeders.Set(pID, timecache.NowUnixNano())

	return nil
}

func (ps *peerStore) DeleteSeeder(_ context.Context, ih bittorrent.InfoHash, p bittorrent.Peer) (err error) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", p).
		Msg("delete seeder")

	sh := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	if sw, ok := sh.swarms.Get(ih); ok {
		if sw.seeders.Del(p.RawString()) {
			sh.numSeeders.Add(decrUint64)
		}
	} else {
		err = storage.ErrResourceDoesNotExist
	}

	return
}

func (ps *peerStore) PutLeecher(_ context.Context, ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", p).
		Msg("put leecher")

	sh := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	sw, pID := sh.ginSwarm(ih), p.RawString()

	if _, exists := sw.leechers.Get(pID); !exists {
		sh.numLeechers.Add(1)
	}

	sw.leechers.Set(pID, timecache.NowUnixNano())

	return nil
}

func (ps *peerStore) DeleteLeecher(_ context.Context, ih bittorrent.InfoHash, p bittorrent.Peer) (err error) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", p).
		Msg("delete leecher")

	sh := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	if sw, ok := sh.swarms.Get(ih); ok {
		if sw.leechers.Del(p.RawString()) {
			sh.numLeechers.Add(decrUint64)
		}
	} else {
		err = storage.ErrResourceDoesNotExist
	}

	return
}

func (ps *peerStore) GraduateLeecher(_ context.Context, ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", p).
		Msg("graduate leecher")

	sh := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	sw, pID := sh.ginSwarm(ih), p.RawString()

	if sw.leechers.Del(pID) {
		sh.numLeechers.Add(decrUint64)
	}

	if _, exists := sw.seeders.Get(pID); !exists {
		sh.numSeeders.Add(1)
	}

	sw.seeders.Set(pID, timecache.NowUnixNano())

	return nil
}

func (ps *peerStore) AnnouncePeers(_ context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Bool("forSeeder", forSeeder).
		Int("numWant", numWant).
		Bool("v6", v6).
		Msg("announce peers")

	if sw, ok := ps.shards[ps.shardIndex(ih, v6)].swarms.Get(ih); ok {
		peers = make([]bittorrent.Peer, 0, numWant/2)
		rangeFn := func(pID string, _ int64) bool {
			p, _ := bittorrent.NewPeer(pID)
			peers = append(peers, p)
			numWant--
			return numWant > 0
		}
		if forSeeder {
			sw.leechers.Range(rangeFn)
		} else {
			sw.seeders.Range(rangeFn)
			if numWant > 0 {
				sw.leechers.Range(rangeFn)
			}
		}
	}

	return
}

func (ps *peerStore) countPeers(ih bittorrent.InfoHash, v6 bool) (leechers, seeders uint32) {
	shard := ps.shards[ps.shardIndex(ih, v6)]

	if sw, ok := shard.swarms.Get(ih); ok {
		leechers, seeders = uint32(sw.leechers.Len()), uint32(sw.seeders.Len())
	}
	return
}

func (ps *peerStore) ScrapeSwarm(_ context.Context, ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Msg("scrape swarm")

	leechers, seeders = ps.countPeers(ih, true)
	l, s := ps.countPeers(ih, false)
	leechers, seeders = leechers+l, seeders+s

	return
}

// NewDataStorage creates new in-memory data store
func NewDataStorage() storage.DataStorage {
	return &dataStore{
		hashmap.New[string, *hashmap.Map[string, []byte]](),
		sync.Mutex{},
	}
}

type dataStore struct {
	*hashmap.Map[string, *hashmap.Map[string, []byte]]
	sync.Mutex
}

func (ds *dataStore) Put(_ context.Context, ctx string, values ...storage.Entry) error {
	if len(values) > 0 {
		m, ok := ds.Get(ctx)
		if !ok {
			ds.Lock()
			if m, ok = ds.Get(ctx); !ok {
				m = hashmap.New[string, []byte]()
				ds.Insert(ctx, m)
			}
			ds.Unlock()
		}
		for _, p := range values {
			m.Set(p.Key, p.Value)
		}
	}
	return nil
}

func (ds *dataStore) Contains(_ context.Context, ctx string, key string) (bool, error) {
	var exist bool
	if m, found := ds.Get(ctx); found {
		_, exist = m.Get(key)
	}
	return exist, nil
}

func (ds *dataStore) Load(_ context.Context, ctx string, key string) (out []byte, _ error) {
	if m, found := ds.Map.Get(ctx); found {
		out, _ = m.Get(key)
	}
	return
}

func (ds *dataStore) Delete(_ context.Context, ctx string, keys ...string) error {
	if len(keys) > 0 {
		if m, found := ds.Get(ctx); found {
			for _, k := range keys {
				m.Del(k)
			}
		}
	}
	return nil
}

func (*dataStore) Preservable() bool { return false }

func (ds *dataStore) Close() error { return nil }

// GC deletes all Peers from the PeerStorage which are older than the
// cutoff time.
//
// This function must be able to execute while other methods on this interface
// are being executed in parallel.
func (ps *peerStore) gc(cutoff time.Time) {
	select {
	case <-ps.closed:
		return
	default:
	}

	cutoffUnix := cutoff.UnixNano()

	for _, shard := range ps.shards {
		infoHashes := make([]bittorrent.InfoHash, 0, shard.swarms.Len())
		shard.swarms.Range(func(ih bittorrent.InfoHash, _ swarm) bool {
			infoHashes = append(infoHashes, ih)
			return true
		})
		runtime.Gosched()

		for _, ih := range infoHashes {
			sw, stillExists := shard.swarms.Get(ih)
			if !stillExists {
				runtime.Gosched()
				continue
			}

			sw.leechers.Range(func(pID string, mtime int64) bool {
				if mtime <= cutoffUnix {
					sw.leechers.Del(pID)
					shard.numLeechers.Add(decrUint64)
				}
				return true
			})

			sw.seeders.Range(func(pID string, mtime int64) bool {
				if mtime <= cutoffUnix {
					sw.seeders.Del(pID)
					shard.numSeeders.Add(decrUint64)
				}
				return true
			})

			if sw.leechers.Len()|sw.seeders.Len() == 0 {
				shard.swarms.Del(ih)
			}

			runtime.Gosched()
		}

		runtime.Gosched()
	}
}

func (*peerStore) Ping(context.Context) error {
	return nil
}

func (ps *peerStore) Close() error {
	ps.onceCloser.Do(func() {
		close(ps.closed)
		ps.wg.Wait()

		// Explicitly deallocate our storage.
		shards := make([]*peerShard, len(ps.shards))
		for i := 0; i < len(ps.shards); i++ {
			shards[i] = &peerShard{swarms: hashmap.New[bittorrent.InfoHash, swarm]()}
		}
		ps.shards = shards
	})

	return nil
}
