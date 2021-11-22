// Package memory implements the storage interface for a Chihaya
// BitTorrent tracker keeping peer data in memory.
package memory

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/pkg/log"
	"github.com/chihaya/chihaya/pkg/stop"
	"github.com/chihaya/chihaya/pkg/timecache"
	"github.com/chihaya/chihaya/storage"
)

// Name is the name by which this peer store is registered with Chihaya.
const Name = "memory"

// Default config constants.
const (
	defaultShardCount                  = 1024
	defaultPrometheusReportingInterval = time.Second * 1
	defaultGarbageCollectionInterval   = time.Minute * 3
	defaultPeerLifetime                = time.Minute * 30
)

func init() {
	// Register the storage driver.
	storage.RegisterDriver(Name, driver{})
}

type driver struct{}

func (d driver) NewStorage(icfg interface{}) (storage.Storage, error) {
	// Marshal the config back into bytes.
	bytes, err := yaml.Marshal(icfg)
	if err != nil {
		return nil, err
	}

	// Unmarshal the bytes into the proper config type.
	var cfg Config
	err = yaml.Unmarshal(bytes, &cfg)
	if err != nil {
		return nil, err
	}

	return New(cfg)
}

// Config holds the configuration of a memory Storage.
type Config struct {
	GarbageCollectionInterval   time.Duration `yaml:"gc_interval"`
	PrometheusReportingInterval time.Duration `yaml:"prometheus_reporting_interval"`
	PeerLifetime                time.Duration `yaml:"peer_lifetime"`
	ShardCount                  int           `yaml:"shard_count"`
}

// LogFields renders the current config as a set of Logrus fields.
func (cfg Config) LogFields() log.Fields {
	return log.Fields{
		"name":               Name,
		"gcInterval":         cfg.GarbageCollectionInterval,
		"promReportInterval": cfg.PrometheusReportingInterval,
		"peerLifetime":       cfg.PeerLifetime,
		"shardCount":         cfg.ShardCount,
	}
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() Config {
	validcfg := cfg

	if cfg.ShardCount <= 0 {
		validcfg.ShardCount = defaultShardCount
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".ShardCount",
			"provided": cfg.ShardCount,
			"default":  validcfg.ShardCount,
		})
	}

	if cfg.GarbageCollectionInterval <= 0 {
		validcfg.GarbageCollectionInterval = defaultGarbageCollectionInterval
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".GarbageCollectionInterval",
			"provided": cfg.GarbageCollectionInterval,
			"default":  validcfg.GarbageCollectionInterval,
		})
	}

	if cfg.PrometheusReportingInterval <= 0 {
		validcfg.PrometheusReportingInterval = defaultPrometheusReportingInterval
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".PrometheusReportingInterval",
			"provided": cfg.PrometheusReportingInterval,
			"default":  validcfg.PrometheusReportingInterval,
		})
	}

	if cfg.PeerLifetime <= 0 {
		validcfg.PeerLifetime = defaultPeerLifetime
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".PeerLifetime",
			"provided": cfg.PeerLifetime,
			"default":  validcfg.PeerLifetime,
		})
	}

	return validcfg
}

// New creates a new Storage backed by memory.
func New(provided Config) (storage.Storage, error) {
	cfg := provided.Validate()
	ps := &store{
		cfg:      cfg,
		shards:   make([]*peerShard, cfg.ShardCount*2),
		contexts: sync.Map{},
		closed:   make(chan struct{}),
	}

	for i := 0; i < cfg.ShardCount*2; i++ {
		ps.shards[i] = &peerShard{swarms: make(map[bittorrent.InfoHash]swarm)}
	}

	// Start a goroutine for garbage collection.
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		for {
			select {
			case <-ps.closed:
				return
			case <-time.After(cfg.GarbageCollectionInterval):
				before := time.Now().Add(-cfg.PeerLifetime)
				log.Debug("storage: purging peers with no announces since", log.Fields{"before": before})
				if err := ps.collectGarbage(before); err != nil {
					log.Error(err)
				}
			}
		}
	}()

	if cfg.PrometheusReportingInterval > 0 {
		// Start a goroutine for reporting statistics to Prometheus.
		ps.wg.Add(1)
		go func() {
			defer ps.wg.Done()
			t := time.NewTicker(cfg.PrometheusReportingInterval)
			for {
				select {
				case <-ps.closed:
					t.Stop()
					return
				case <-t.C:
					before := time.Now()
					ps.populateProm()
					log.Debug("storage: populateProm() finished", log.Fields{"timeTaken": time.Since(before)})
				}
			}
		}()
	} else {
		log.Info("prometheus disabled because of zero reporting interval")
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
	seeders  map[storage.SerializedPeer]int64
	leechers map[storage.SerializedPeer]int64
}

type store struct {
	cfg      Config
	shards   []*peerShard
	contexts sync.Map

	closed chan struct{}
	wg     sync.WaitGroup
}

var _ storage.Storage = &store{}

// populateProm aggregates metrics over all shards and then posts them to
// prometheus.
func (ps *store) populateProm() {
	var numInfohashes, numSeeders, numLeechers uint64

	for _, s := range ps.shards {
		s.RLock()
		numInfohashes += uint64(len(s.swarms))
		numSeeders += s.numSeeders
		numLeechers += s.numLeechers
		s.RUnlock()
	}

	storage.PromInfohashesCount.Set(float64(numInfohashes))
	storage.PromSeedersCount.Set(float64(numSeeders))
	storage.PromLeechersCount.Set(float64(numLeechers))
}

// recordGCDuration records the duration of a GC sweep.
func recordGCDuration(duration time.Duration) {
	storage.PromGCDurationMilliseconds.Observe(float64(duration.Nanoseconds()) / float64(time.Millisecond))
}

func (ps *store) getClock() int64 {
	return timecache.NowUnixNano()
}

func (ps *store) shardIndex(infoHash bittorrent.InfoHash, af bittorrent.AddressFamily) uint32 {
	// There are twice the amount of shards specified by the user, the first
	// half is dedicated to IPv4 swarms and the second half is dedicated to
	// IPv6 swarms.
	idx := binary.BigEndian.Uint32([]byte(infoHash[:4])) % (uint32(len(ps.shards)) / 2)
	if af == bittorrent.IPv6 {
		idx += uint32(len(ps.shards) / 2)
	}
	return idx
}

func (ps *store) PutSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := storage.NewSerializedPeer(p)

	shard := ps.shards[ps.shardIndex(ih, p.IP.AddressFamily)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[storage.SerializedPeer]int64),
			leechers: make(map[storage.SerializedPeer]int64),
		}
	}

	// If this peer isn't already a seeder, update the stats for the swarm.
	if _, ok := shard.swarms[ih].seeders[pk]; !ok {
		shard.numSeeders++
	}

	// Update the peer in the swarm.
	shard.swarms[ih].seeders[pk] = ps.getClock()

	shard.Unlock()
	return nil
}

func (ps *store) DeleteSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := storage.NewSerializedPeer(p)

	shard := ps.shards[ps.shardIndex(ih, p.IP.AddressFamily)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.Unlock()
		return storage.ErrResourceDoesNotExist
	}

	if _, ok := shard.swarms[ih].seeders[pk]; !ok {
		shard.Unlock()
		return storage.ErrResourceDoesNotExist
	}

	shard.numSeeders--
	delete(shard.swarms[ih].seeders, pk)

	if len(shard.swarms[ih].seeders)|len(shard.swarms[ih].leechers) == 0 {
		delete(shard.swarms, ih)
	}

	shard.Unlock()
	return nil
}

func (ps *store) PutLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := storage.NewSerializedPeer(p)

	shard := ps.shards[ps.shardIndex(ih, p.IP.AddressFamily)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[storage.SerializedPeer]int64),
			leechers: make(map[storage.SerializedPeer]int64),
		}
	}

	// If this peer isn't already a leecher, update the stats for the swarm.
	if _, ok := shard.swarms[ih].leechers[pk]; !ok {
		shard.numLeechers++
	}

	// Update the peer in the swarm.
	shard.swarms[ih].leechers[pk] = ps.getClock()

	shard.Unlock()
	return nil
}

func (ps *store) DeleteLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := storage.NewSerializedPeer(p)

	shard := ps.shards[ps.shardIndex(ih, p.IP.AddressFamily)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.Unlock()
		return storage.ErrResourceDoesNotExist
	}

	if _, ok := shard.swarms[ih].leechers[pk]; !ok {
		shard.Unlock()
		return storage.ErrResourceDoesNotExist
	}

	shard.numLeechers--
	delete(shard.swarms[ih].leechers, pk)

	if len(shard.swarms[ih].seeders)|len(shard.swarms[ih].leechers) == 0 {
		delete(shard.swarms, ih)
	}

	shard.Unlock()
	return nil
}

func (ps *store) GraduateLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := storage.NewSerializedPeer(p)

	shard := ps.shards[ps.shardIndex(ih, p.IP.AddressFamily)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[storage.SerializedPeer]int64),
			leechers: make(map[storage.SerializedPeer]int64),
		}
	}

	// If this peer is a leecher, update the stats for the swarm and remove them.
	if _, ok := shard.swarms[ih].leechers[pk]; ok {
		shard.numLeechers--
		delete(shard.swarms[ih].leechers, pk)
	}

	// If this peer isn't already a seeder, update the stats for the swarm.
	if _, ok := shard.swarms[ih].seeders[pk]; !ok {
		shard.numSeeders++
	}

	// Update the peer in the swarm.
	shard.swarms[ih].seeders[pk] = ps.getClock()

	shard.Unlock()
	return nil
}

func (ps *store) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, announcer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	shard := ps.shards[ps.shardIndex(ih, announcer.IP.AddressFamily)]
	shard.RLock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.RUnlock()
		return nil, storage.ErrResourceDoesNotExist
	}

	if seeder {
		// Append leechers as possible.
		leechers := shard.swarms[ih].leechers
		for pk := range leechers {
			if numWant == 0 {
				break
			}

			peers = append(peers, pk.ToPeer())
			numWant--
		}
	} else {
		// Append as many seeders as possible.
		seeders := shard.swarms[ih].seeders
		for pk := range seeders {
			if numWant == 0 {
				break
			}

			peers = append(peers, pk.ToPeer())
			numWant--
		}

		// Append leechers until we reach numWant.
		if numWant > 0 {
			leechers := shard.swarms[ih].leechers
			announcerPK := storage.NewSerializedPeer(announcer)
			for pk := range leechers {
				if pk == announcerPK {
					continue
				}

				if numWant == 0 {
					break
				}

				peers = append(peers, pk.ToPeer())
				numWant--
			}
		}
	}

	shard.RUnlock()
	return
}

func (ps *store) ScrapeSwarm(ih bittorrent.InfoHash, addressFamily bittorrent.AddressFamily) (resp bittorrent.Scrape) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	resp.InfoHash = ih
	shard := ps.shards[ps.shardIndex(ih, addressFamily)]
	shard.RLock()

	swarm, ok := shard.swarms[ih]
	if !ok {
		shard.RUnlock()
		return
	}

	resp.Incomplete = uint32(len(swarm.leechers))
	resp.Complete = uint32(len(swarm.seeders))
	shard.RUnlock()

	return
}

func asKey(in interface{}) interface{} {
	if in == nil {
		panic("unable to use nil map key")
	}
	if reflect.TypeOf(in).Comparable() {
		return in
	}
	//FIXME: dirty hack
	return fmt.Sprint(in)
}

func (ps *store) Put(ctx string, key, value interface{}) {
	m, _ := ps.contexts.LoadOrStore(ctx, new(sync.Map))
	m.(*sync.Map).Store(asKey(key), value)
}

func (ps *store) Contains(ctx string, key interface{}) bool {
	var exist bool
	if m, found := ps.contexts.Load(ctx); found {
		_, exist = m.(*sync.Map).Load(asKey(key))
	}
	return exist
}

func (ps *store) BulkPut(ctx string, pairs ...storage.Pair) {
	if len(pairs) > 0 {
		c, _ := ps.contexts.LoadOrStore(ctx, new(sync.Map))
		m := c.(*sync.Map)
		for _, p := range pairs {
			m.Store(asKey(p.Left), p.Right)
		}
	}
}

func (ps *store) Load(ctx string, key interface{}) interface{} {
	var v interface{}
	if m, found := ps.contexts.Load(ctx); found {
		v, _ = m.(*sync.Map).Load(asKey(key))
	}
	return v
}

func (ps *store) Delete(ctx string, keys ...interface{}) {
	if len(keys) > 0 {
		if m, found := ps.contexts.Load(ctx); found {
			m := m.(*sync.Map)
			for k := range keys {
				m.Delete(asKey(k))
			}
		}
	}
}

// collectGarbage deletes all Peers from the Storage which are older than the
// cutoff time.
//
// This function must be able to execute while other methods on this interface
// are being executed in parallel.
func (ps *store) collectGarbage(cutoff time.Time) error {
	select {
	case <-ps.closed:
		return nil
	default:
	}

	cutoffUnix := cutoff.UnixNano()
	start := time.Now()

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

	recordGCDuration(time.Since(start))

	return nil
}

func (ps *store) Stop() stop.Result {
	c := make(stop.Channel)
	go func() {
		close(ps.closed)
		ps.wg.Wait()

		// Explicitly deallocate our storage.
		shards := make([]*peerShard, len(ps.shards))
		for i := 0; i < len(ps.shards); i++ {
			shards[i] = &peerShard{swarms: make(map[bittorrent.InfoHash]swarm)}
		}
		ps.shards = shards

		c.Done()
	}()

	return c.Result()
}

func (ps *store) LogFields() log.Fields {
	return ps.cfg.LogFields()
}
