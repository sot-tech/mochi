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
	storage.RegisterDriver(Name, Builder{})
}

type Builder struct{}

func (Builder) NewDataStorage(conf.MapConfig) (storage.DataStorage, error) {
	return dataStorage(), nil
}

func (Builder) NewPeerStorage(icfg conf.MapConfig) (storage.PeerStorage, error) {
	var cfg config
	if err := icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return peerStorage(cfg)
}

type config struct {
	ShardCount int `cfg:"shard_count"`
}

func (cfg config) validate() config {
	validcfg := cfg

	if cfg.ShardCount <= 0 || cfg.ShardCount > (math.MaxInt/2) {
		validcfg.ShardCount = defaultShardCount
		logger.Warn().
			Str("name", "ShardCount").
			Int("provided", cfg.ShardCount).
			Int("default", validcfg.ShardCount).
			Msg("falling back to default configuration")
	}

	return validcfg
}

func peerStorage(provided config) (storage.PeerStorage, error) {
	cfg := provided.validate()
	ps := &peerStore{
		shards:      make([]*peerShard, cfg.ShardCount*2),
		DataStorage: dataStorage(),
		closed:      make(chan any),
	}

	for i := 0; i < cfg.ShardCount*2; i++ {
		ps.shards[i] = &peerShard{swarms: &ihSwarm{m: make(map[bittorrent.InfoHash]swarm)}}
	}

	return ps, nil
}

type peerShard struct {
	swarms      *ihSwarm
	numSeeders  atomic.Uint64
	numLeechers atomic.Uint64
}

type ihSwarm struct {
	m map[bittorrent.InfoHash]swarm
	sync.RWMutex
}

func (p *ihSwarm) get(k bittorrent.InfoHash) (v swarm, ok bool) {
	p.RLock()
	v, ok = p.m[k]
	p.RUnlock()
	return
}

func (p *ihSwarm) getOrCreate(k bittorrent.InfoHash) (v swarm) {
	var ok bool
	if v, ok = p.get(k); !ok {
		p.Lock()
		if v, ok = p.m[k]; !ok {
			v = swarm{
				seeders:  &peers{m: make(map[bittorrent.Peer]int64)},
				leechers: &peers{m: make(map[bittorrent.Peer]int64)},
			}
			p.m[k] = v
		}
		p.Unlock()
	}
	return
}

func (p *ihSwarm) del(k bittorrent.InfoHash) (ok bool) {
	p.Lock()
	if _, ok = p.m[k]; ok {
		delete(p.m, k)
	}
	p.Unlock()
	return
}

func (p *ihSwarm) len() int {
	return len(p.m)
}

func (p *ihSwarm) keys(fn func(k bittorrent.InfoHash) bool) {
	p.RLock()
	for k := range p.m {
		if !fn(k) {
			break
		}
	}
	p.RUnlock()
}

type swarm struct {
	// map serialized peer to mtime
	seeders  *peers
	leechers *peers
}

type peers struct {
	m map[bittorrent.Peer]int64
	sync.RWMutex
}

func (p *peers) get(k bittorrent.Peer) (v int64, ok bool) {
	p.RLock()
	v, ok = p.m[k]
	p.RUnlock()
	return
}

func (p *peers) set(k bittorrent.Peer, v int64) {
	p.Lock()
	p.m[k] = v
	p.Unlock()
}

func (p *peers) del(k bittorrent.Peer) (ok bool) {
	p.Lock()
	if _, ok = p.m[k]; ok {
		delete(p.m, k)
	}
	p.Unlock()
	return
}

func (p *peers) len() int {
	return len(p.m)
}

func (p *peers) keys(fn func(k bittorrent.Peer) bool) bool {
	p.RLock()
	defer p.RUnlock()
	for k := range p.m {
		if !fn(k) {
			return false
		}
	}
	return true
}

func (p *peers) forEach(fn func(k bittorrent.Peer, v int64) bool) {
	p.RLock()
	for k, v := range p.m {
		if !fn(k, v) {
			break
		}
	}
	p.RUnlock()
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
						numInfoHashes += uint64(s.swarms.len())
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
	idx := binary.BigEndian.Uint32(infoHash.Bytes()[:4]) % (uint32(len(ps.shards)) / 2)
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
	sw := sh.swarms.getOrCreate(ih)

	if _, exists := sw.seeders.get(p); !exists {
		sh.numSeeders.Add(1)
	}

	sw.seeders.set(p, timecache.NowUnixNano())

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
	if sw, ok := sh.swarms.get(ih); ok {
		if sw.seeders.del(p) {
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
	sw := sh.swarms.getOrCreate(ih)

	if _, exists := sw.leechers.get(p); !exists {
		sh.numLeechers.Add(1)
	}

	sw.leechers.set(p, timecache.NowUnixNano())

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
	if sw, ok := sh.swarms.get(ih); ok {
		if sw.leechers.del(p) {
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
	sw := sh.swarms.getOrCreate(ih)

	if sw.leechers.del(p) {
		sh.numLeechers.Add(decrUint64)
	}

	if _, exists := sw.seeders.get(p); !exists {
		sh.numSeeders.Add(1)
	}

	sw.seeders.set(p, timecache.NowUnixNano())

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

	if sw, ok := ps.shards[ps.shardIndex(ih, v6)].swarms.get(ih); ok {
		peers = make([]bittorrent.Peer, 0, numWant/2)
		rangeFn := func(p bittorrent.Peer) bool {
			peers = append(peers, p)
			numWant--
			return numWant > 0
		}
		if forSeeder {
			sw.leechers.keys(rangeFn)
		} else {
			if sw.seeders.keys(rangeFn) {
				sw.leechers.keys(rangeFn)
			}
		}
	}

	return
}

func (ps *peerStore) countPeers(ih bittorrent.InfoHash, v6 bool) (leechers, seeders uint32) {
	shard := ps.shards[ps.shardIndex(ih, v6)]

	if sw, ok := shard.swarms.get(ih); ok {
		leechers, seeders = uint32(sw.leechers.len()), uint32(sw.seeders.len())
	}
	return
}

func (ps *peerStore) ScrapeSwarm(_ context.Context, ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32, _ error) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Msg("scrape swarm")

	leechers, seeders = ps.countPeers(ih, false)
	l, s := ps.countPeers(ih, true)
	leechers, seeders = leechers+l, seeders+s

	return
}

func dataStorage() storage.DataStorage {
	return new(dataStore)
}

type dataStore struct {
	sync.Map
}

func (ds *dataStore) Put(_ context.Context, ctx string, values ...storage.Entry) error {
	if len(values) > 0 {
		c, _ := ds.LoadOrStore(ctx, new(sync.Map))
		m := c.(*sync.Map)
		for _, p := range values {
			m.Store(p.Key, p.Value)
		}
	}
	return nil
}

func (ds *dataStore) Contains(_ context.Context, ctx string, key string) (bool, error) {
	var exist bool
	if m, found := ds.Map.Load(ctx); found {
		_, exist = m.(*sync.Map).Load(key)
	}
	return exist, nil
}

func (ds *dataStore) Load(_ context.Context, ctx string, key string) (out []byte, _ error) {
	if m, found := ds.Map.Load(ctx); found {
		if v, _ := m.(*sync.Map).Load(key); v != nil {
			out = v.([]byte)
		}
	}
	return
}

func (ds *dataStore) Delete(_ context.Context, ctx string, keys ...string) error {
	if len(keys) > 0 {
		if m, found := ds.Map.Load(ctx); found {
			m := m.(*sync.Map)
			for _, k := range keys {
				m.Delete(k)
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

	toDel := make([]bittorrent.Peer, 0, len(ps.shards)/5)

	for _, shard := range ps.shards {
		infoHashes := make([]bittorrent.InfoHash, 0, shard.swarms.len())
		shard.swarms.keys(func(ih bittorrent.InfoHash) bool {
			infoHashes = append(infoHashes, ih)
			return true
		})
		runtime.Gosched()

		for _, ih := range infoHashes {
			sw, stillExists := shard.swarms.get(ih)
			if !stillExists {
				runtime.Gosched()
				continue
			}

			sw.leechers.forEach(func(p bittorrent.Peer, mtime int64) bool {
				if mtime <= cutoffUnix {
					toDel = append(toDel, p)
				}
				return true
			})

			for _, p := range toDel {
				if sw.leechers.del(p) {
					shard.numLeechers.Add(decrUint64)
				}
			}

			toDel = toDel[:0]

			sw.seeders.forEach(func(p bittorrent.Peer, mtime int64) bool {
				if mtime <= cutoffUnix {
					toDel = append(toDel, p)
				}
				return true
			})

			for _, p := range toDel {
				if sw.seeders.del(p) {
					shard.numSeeders.Add(decrUint64)
				}
			}

			toDel = toDel[:0]

			if sw.leechers.len()|sw.seeders.len() == 0 {
				shard.swarms.del(ih)
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
	})

	return nil
}
