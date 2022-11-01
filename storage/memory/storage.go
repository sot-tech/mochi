// Package memory implements the storage interface for a Conf
// BitTorrent tracker keeping peer data in memory.
package memory

import (
	"context"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

// Default config constants.
const defaultShardCount = 1024

var logger = log.NewLogger("storage/memory")

func init() {
	// Register the storage driver.
	storage.RegisterDriver("memory", builder)
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
		cfg:         cfg,
		shards:      make([]*peerShard, cfg.ShardCount*2),
		DataStorage: NewDataStorage(),
		closed:      make(chan struct{}),
	}

	for i := 0; i < cfg.ShardCount*2; i++ {
		ps.shards[i] = &peerShard{swarms: make(map[bittorrent.InfoHash]swarm)}
	}

	return ps, nil
}

type peerShard struct {
	swarms      map[bittorrent.InfoHash]swarm
	numSeeders  uint64
	numLeechers uint64
	sync.RWMutex
}

type swarm struct {
	// map serialized peer to mtime
	seeders  map[bittorrent.Peer]int64
	leechers map[bittorrent.Peer]int64
}

type peerStore struct {
	storage.DataStorage
	cfg    Config
	shards []*peerShard

	closed     chan struct{}
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
					var numInfohashes, numSeeders, numLeechers uint64

					for _, s := range ps.shards {
						s.RLock()
						numInfohashes += uint64(len(s.swarms))
						numSeeders += s.numSeeders
						numLeechers += s.numLeechers
						s.RUnlock()
					}

					storage.PromInfoHashesCount.Set(float64(numInfohashes))
					storage.PromSeedersCount.Set(float64(numSeeders))
					storage.PromLeechersCount.Set(float64(numLeechers))
					logger.Debug().TimeDiff("timeTaken", time.Now(), before).Msg("populate prom complete")
				}
			}
		}
	}()
}

func (ps *peerStore) getClock() int64 {
	return timecache.NowUnixNano()
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

	shard := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	shard.Lock()
	defer shard.Unlock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[bittorrent.Peer]int64),
			leechers: make(map[bittorrent.Peer]int64),
		}
	}

	// If this peer isn't already a seeder, update the stats for the swarm.
	if _, ok := shard.swarms[ih].seeders[p]; !ok {
		shard.numSeeders++
	}

	// Update the peer in the swarm.
	shard.swarms[ih].seeders[p] = ps.getClock()

	return nil
}

func (ps *peerStore) DeleteSeeder(_ context.Context, ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", p).
		Msg("delete seeder")

	shard := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	shard.Lock()
	defer shard.Unlock()

	if _, ok := shard.swarms[ih]; !ok {
		return storage.ErrResourceDoesNotExist
	}

	if _, ok := shard.swarms[ih].seeders[p]; !ok {
		return storage.ErrResourceDoesNotExist
	}

	shard.numSeeders--
	delete(shard.swarms[ih].seeders, p)

	if len(shard.swarms[ih].seeders)|len(shard.swarms[ih].leechers) == 0 {
		delete(shard.swarms, ih)
	}

	return nil
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

	shard := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	shard.Lock()
	defer shard.Unlock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[bittorrent.Peer]int64),
			leechers: make(map[bittorrent.Peer]int64),
		}
	}

	// If this peer isn't already a leecher, update the stats for the swarm.
	if _, ok := shard.swarms[ih].leechers[p]; !ok {
		shard.numLeechers++
	}

	// Update the peer in the swarm.
	shard.swarms[ih].leechers[p] = ps.getClock()

	return nil
}

func (ps *peerStore) DeleteLeecher(_ context.Context, ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", p).
		Msg("delete leecher")

	shard := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	shard.Lock()
	defer shard.Unlock()

	if _, ok := shard.swarms[ih]; !ok {
		return storage.ErrResourceDoesNotExist
	}

	if _, ok := shard.swarms[ih].leechers[p]; !ok {
		return storage.ErrResourceDoesNotExist
	}

	shard.numLeechers--
	delete(shard.swarms[ih].leechers, p)

	if len(shard.swarms[ih].seeders)|len(shard.swarms[ih].leechers) == 0 {
		delete(shard.swarms, ih)
	}

	return nil
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

	shard := ps.shards[ps.shardIndex(ih, p.Addr().Is6())]
	shard.Lock()
	defer shard.Unlock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[bittorrent.Peer]int64),
			leechers: make(map[bittorrent.Peer]int64),
		}
	}

	// If this peer is a leecher, update the stats for the swarm and remove them.
	if _, ok := shard.swarms[ih].leechers[p]; ok {
		shard.numLeechers--
		delete(shard.swarms[ih].leechers, p)
	}

	// If this peer isn't already a seeder, update the stats for the swarm.
	if _, ok := shard.swarms[ih].seeders[p]; !ok {
		shard.numSeeders++
	}

	// Update the peer in the swarm.
	shard.swarms[ih].seeders[p] = ps.getClock()

	return nil
}

func parsePeers(peersMap map[bittorrent.Peer]int64, maxCount int) (peers []bittorrent.Peer) {
	for p := range peersMap {
		if maxCount == 0 {
			break
		}
		peers = append(peers, p)
		maxCount--
	}
	return
}

func (ps *peerStore) getPeers(shard *peerShard, ih bittorrent.InfoHash, maxCount int, forSeeder bool) (peers []bittorrent.Peer) {
	shard.RLock()
	defer shard.RUnlock()
	if swarm, ok := shard.swarms[ih]; ok {
		if forSeeder {
			peers = parsePeers(swarm.leechers, maxCount)
		} else {
			peers = append(peers, parsePeers(swarm.seeders, maxCount)...)
			if maxCount -= len(peers); maxCount > 0 {
				peers = append(peers, parsePeers(swarm.leechers, maxCount)...)
			}
		}
	}
	return
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

	peers = ps.getPeers(ps.shards[ps.shardIndex(ih, v6)], ih, numWant, forSeeder)

	return
}

func (ps *peerStore) countPeers(ih bittorrent.InfoHash, v6 bool) (leechers, seeders uint32) {
	shard := ps.shards[ps.shardIndex(ih, v6)]
	shard.RLock()
	defer shard.RUnlock()

	if swarm, ok := shard.swarms[ih]; ok {
		leechers, seeders = uint32(len(swarm.leechers)), uint32(len(swarm.seeders))
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

	for _, shard := range ps.shards {
		shard.RLock()
		var infohashes []bittorrent.InfoHash
		for ih := range shard.swarms {
			infohashes = append(infohashes, ih)
		}
		shard.RUnlock()
		runtime.Gosched()

		for _, ih := range infohashes {
			shard.Lock()

			if _, stillExists := shard.swarms[ih]; !stillExists {
				shard.Unlock()
				runtime.Gosched()
				continue
			}

			for pk, mtime := range shard.swarms[ih].leechers {
				if mtime <= cutoffUnix {
					shard.numLeechers--
					delete(shard.swarms[ih].leechers, pk)
				}
			}

			for pk, mtime := range shard.swarms[ih].seeders {
				if mtime <= cutoffUnix {
					shard.numSeeders--
					delete(shard.swarms[ih].seeders, pk)
				}
			}

			if len(shard.swarms[ih].seeders)|len(shard.swarms[ih].leechers) == 0 {
				delete(shard.swarms, ih)
			}

			shard.Unlock()
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
			shards[i] = &peerShard{swarms: make(map[bittorrent.InfoHash]swarm)}
		}
		ps.shards = shards
	})

	return nil
}
