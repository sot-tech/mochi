// Package redis implements the storage interface for a Conf
// BitTorrent tracker keeping peer data in redis with hash.
// There two categories of hash:
//
// - CHI_{4,6}_{L,S}_<HASH> (hash type)
//	To save peers that hold the infohash, used for fast searching,
//  deleting, and timeout handling
//
// - CHI_{4,6}_I (set type)
//  To save all the infohashes, used for garbage collection,
//	metrics aggregation and leecher graduation
//
// Tree keys are used to record the count of swarms, seeders
// and leechers for each group (IPv4, IPv6).
//
// - CHI_{4,6}_S_C (key type)
//	To record the number of seeders.
//
// - CHI_{4,6}_L_C (key type)
//	To record the number of leechers.
package redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

// Name is the name by which this peer store is registered with Conf.
const Name = "redis"

// Default config constants.
const (
	defaultPrometheusReportingInterval = time.Second * 1
	defaultGarbageCollectionInterval   = time.Minute * 3
	defaultPeerLifetime                = time.Minute * 30
	defaultRedisAddress                = "127.0.0.1:6379"
	defaultReadTimeout                 = time.Second * 15
	defaultWriteTimeout                = time.Second * 15
	defaultConnectTimeout              = time.Second * 15
	prefixKey                          = "CHI_"
	ih4Key                             = "CHI_4_I"
	ih6Key                             = "CHI_6_I"
	ih4SeederKey                       = "CHI_4_S_"
	ih6SeederKey                       = "CHI_6_S_"
	ih4LeecherKey                      = "CHI_4_L_"
	ih6LeecherKey                      = "CHI_6_L_"
	cnt4SeederKey                      = "CHI_4_C_S"
	cnt6SeederKey                      = "CHI_6_C_S"
	cnt4LeecherKey                     = "CHI_4_C_L"
	cnt6LeecherKey                     = "CHI_6_C_L"
)

// ErrSentinelAndClusterChecked returned from initializer if both Config.Sentinel and Config.Cluster provided
var ErrSentinelAndClusterChecked = errors.New("unable to use both cluster and sentinel mode")

func init() {
	// Register the storage driver.
	storage.RegisterDriver(Name, driver{})
}

type driver struct{}

func (d driver) NewStorage(icfg conf.MapConfig) (storage.Storage, error) {
	// Unmarshal the bytes into the proper config type.
	var cfg Config

	if err := icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return New(cfg)
}

// Config holds the configuration of a redis Storage.
type Config struct {
	GarbageCollectionInterval   time.Duration `cfg:"gc_interval"`
	PrometheusReportingInterval time.Duration `cfg:"prometheus_reporting_interval"`
	PeerLifetime                time.Duration `cfg:"peer_lifetime"`
	Addresses                   []string
	DB                          int
	PoolSize                    int `cfg:"pool_size"`
	Login                       string
	Password                    string
	Sentinel                    bool
	SentinelMaster              string `cfg:"sentinel_master"`
	Cluster                     bool
	ReadTimeout                 time.Duration `cfg:"read_timeout"`
	WriteTimeout                time.Duration `cfg:"write_timeout"`
	ConnectTimeout              time.Duration `cfg:"connect_timeout"`
}

// LogFields renders the current config as a set of Logrus fields.
func (cfg Config) LogFields() log.Fields {
	return log.Fields{
		"name":               Name,
		"gcInterval":         cfg.GarbageCollectionInterval,
		"promReportInterval": cfg.PrometheusReportingInterval,
		"peerLifetime":       cfg.PeerLifetime,
		"addresses":          cfg.Addresses,
		"readTimeout":        cfg.ReadTimeout,
		"writeTimeout":       cfg.WriteTimeout,
		"connectTimeout":     cfg.ConnectTimeout,
	}
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() (Config, error) {
	if cfg.Sentinel && cfg.Cluster {
		return cfg, ErrSentinelAndClusterChecked
	}

	validCfg := cfg

	addresses := make([]string, 0)
	if n := len(cfg.Addresses); n > 0 {
		for _, a := range cfg.Addresses {
			if len(strings.TrimSpace(a)) > 0 {
				addresses = append(addresses, a)
			}
		}
	}
	validCfg.Addresses = addresses
	if len(cfg.Addresses) == 0 {
		validCfg.Addresses = []string{defaultRedisAddress}
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".Addresses",
			"provided": cfg.Addresses,
			"default":  validCfg.Addresses,
		})
	}

	if cfg.ReadTimeout <= 0 {
		validCfg.ReadTimeout = defaultReadTimeout
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".ReadTimeout",
			"provided": cfg.ReadTimeout,
			"default":  validCfg.ReadTimeout,
		})
	}

	if cfg.WriteTimeout <= 0 {
		validCfg.WriteTimeout = defaultWriteTimeout
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".WriteTimeout",
			"provided": cfg.WriteTimeout,
			"default":  validCfg.WriteTimeout,
		})
	}

	if cfg.ConnectTimeout <= 0 {
		validCfg.ConnectTimeout = defaultConnectTimeout
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".ConnectTimeout",
			"provided": cfg.ConnectTimeout,
			"default":  validCfg.ConnectTimeout,
		})
	}

	if cfg.GarbageCollectionInterval <= 0 {
		validCfg.GarbageCollectionInterval = defaultGarbageCollectionInterval
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".GarbageCollectionInterval",
			"provided": cfg.GarbageCollectionInterval,
			"default":  validCfg.GarbageCollectionInterval,
		})
	}

	if cfg.PrometheusReportingInterval < 0 {
		validCfg.PrometheusReportingInterval = defaultPrometheusReportingInterval
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".PrometheusReportingInterval",
			"provided": cfg.PrometheusReportingInterval,
			"default":  validCfg.PrometheusReportingInterval,
		})
	}

	if cfg.PeerLifetime <= 0 {
		validCfg.PeerLifetime = defaultPeerLifetime
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".PeerLifetime",
			"provided": cfg.PeerLifetime,
			"default":  validCfg.PeerLifetime,
		})
	}

	return validCfg, nil
}

func connect(cfg Config) (*store, error) {
	var err error
	db := &store{
		// FIXME: get context from parent and put into GC, middleware functions should use own ctx
		ctx: context.TODO(),
	}
	switch {
	case cfg.Cluster:
		db.con = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.Addresses,
			Username:     cfg.Login,
			Password:     cfg.Password,
			DialTimeout:  cfg.ConnectTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			PoolSize:     cfg.PoolSize,
		})
	case cfg.Sentinel:
		db.con = redis.NewFailoverClient(&redis.FailoverOptions{
			SentinelAddrs:    cfg.Addresses,
			SentinelUsername: cfg.Login,
			SentinelPassword: cfg.Password,
			MasterName:       cfg.SentinelMaster,
			DialTimeout:      cfg.ConnectTimeout,
			ReadTimeout:      cfg.ReadTimeout,
			WriteTimeout:     cfg.WriteTimeout,
			PoolSize:         cfg.PoolSize,
			DB:               cfg.DB,
		})
	default:
		db.con = redis.NewClient(&redis.Options{
			Addr:         cfg.Addresses[0],
			Username:     cfg.Login,
			Password:     cfg.Password,
			DialTimeout:  cfg.ConnectTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			PoolSize:     cfg.PoolSize,
			DB:           cfg.DB,
		})
	}
	if err = db.con.Ping(db.ctx).Err(); err == nil && !errors.Is(err, redis.Nil) {
		err = nil
	} else {
		_ = db.con.Close()
		db = nil
	}
	return db, err
}

// New creates a new Storage backed by redis.
func New(conf Config) (storage.Storage, error) {
	cfg, err := conf.Validate()
	if err != nil {
		return nil, err
	}

	ps, err := connect(cfg)
	if err != nil {
		return nil, err
	}
	ps.closed = make(chan any)
	ps.logFields = cfg.LogFields()

	// Start a goroutine for garbage collection.
	ps.wg.Add(1)
	go ps.scheduleGC(cfg.GarbageCollectionInterval, cfg.PeerLifetime)

	if cfg.PrometheusReportingInterval > 0 {
		// Start a goroutine for reporting statistics to Prometheus.
		ps.wg.Add(1)
		go ps.schedulerProm(cfg.PrometheusReportingInterval)
	} else {
		log.Info("prometheus disabled because of zero reporting interval")
	}

	return ps, nil
}

func (ps *store) scheduleGC(gcInterval, peerLifeTime time.Duration) {
	defer ps.wg.Done()
	t := time.NewTimer(gcInterval)
	defer t.Stop()
	for {
		select {
		case <-ps.closed:
			return
		case <-t.C:
			start := time.Now()
			ps.GC(time.Now().Add(-peerLifeTime))
			duration := time.Since(start).Milliseconds()
			log.Debug("storage: recordGCDuration", log.Fields{"timeTaken(ms)": duration})
			storage.PromGCDurationMilliseconds.Observe(float64(duration))
			t.Reset(gcInterval)
		}
	}
}

func (ps *store) schedulerProm(reportInterval time.Duration) {
	defer ps.wg.Done()
	t := time.NewTicker(reportInterval)
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
}

type store struct {
	con redis.UniversalClient
	ctx context.Context

	closed    chan any
	wg        sync.WaitGroup
	logFields log.Fields
}

func (ps *store) count(key string, getLength bool) (n uint64) {
	var err error
	if getLength {
		n, err = ps.con.SCard(ps.ctx, key).Uint64()
	} else {
		n, err = ps.con.Get(ps.ctx, key).Uint64()
	}
	err = asNil(err)
	if err != nil {
		log.Error("storage: len/counter failure", log.Fields{
			"key":   key,
			"error": err,
		})
	}
	return
}

// populateProm aggregates metrics over all groups and then posts them to
// prometheus.
func (ps *store) populateProm() {
	numInfoHashes, numSeeders, numLeechers := new(uint64), new(uint64), new(uint64)
	fetchFn := func(v6 bool) {
		var cntSeederKey, cntLeecherKey, ihSummaryKey string
		if v6 {
			cntSeederKey, cntLeecherKey, ihSummaryKey = cnt6SeederKey, cnt6LeecherKey, ih6Key
		} else {
			cntSeederKey, cntLeecherKey, ihSummaryKey = cnt4SeederKey, cnt4LeecherKey, ih4Key
		}
		*numInfoHashes += ps.count(ihSummaryKey, true)
		*numSeeders += ps.count(cntSeederKey, false)
		*numLeechers += ps.count(cntLeecherKey, false)
	}

	fetchFn(false)
	fetchFn(true)

	storage.PromInfoHashesCount.Set(float64(*numInfoHashes))
	storage.PromSeedersCount.Set(float64(*numSeeders))
	storage.PromLeechersCount.Set(float64(*numLeechers))
}

func (ps *store) getClock() int64 {
	return timecache.NowUnixNano()
}

func (ps *store) tx(txf func(tx redis.Pipeliner) error) (err error) {
	if pipe, txErr := ps.con.TxPipelined(ps.ctx, txf); txErr == nil {
		errs := make([]string, 0)
		for _, c := range pipe {
			if err := c.Err(); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if len(errs) > 0 {
			err = errors.New(strings.Join(errs, "; "))
		}
	} else {
		err = txErr
	}
	return
}

func asNil(err error) error {
	if err == nil || errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

func (ps *store) PutSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	var ihSummaryKey, ihPeerKey, cntPeerKey string
	log.Debug("storage: PutSeeder", log.Fields{
		"InfoHash": ih,
		"Peer":     peer,
	})
	if peer.Addr().Is6() {
		ihSummaryKey, ihPeerKey, cntPeerKey = ih6Key, ih6SeederKey, cnt6SeederKey
	} else {
		ihSummaryKey, ihPeerKey, cntPeerKey = ih4Key, ih4SeederKey, cnt4SeederKey
	}
	ihPeerKey += ih.RawString()

	return ps.tx(func(tx redis.Pipeliner) (err error) {
		if err = tx.HSet(ps.ctx, ihPeerKey, peer.RawString(), ps.getClock()).Err(); err != nil {
			return
		}
		if err = ps.con.Incr(ps.ctx, cntPeerKey).Err(); err != nil {
			return
		}
		err = ps.con.SAdd(ps.ctx, ihSummaryKey, ihPeerKey).Err()
		return
	})
}

func (ps *store) DeleteSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	var ihPeerKey, cntPeerKey string
	log.Debug("storage: DeleteSeeder", log.Fields{
		"InfoHash": ih,
		"Peer":     peer,
	})
	if peer.Addr().Is6() {
		ihPeerKey, cntPeerKey = ih6SeederKey, cnt6SeederKey
	} else {
		ihPeerKey, cntPeerKey = ih4SeederKey, cnt4SeederKey
	}
	ihPeerKey += ih.RawString()

	deleted, err := ps.con.HDel(ps.ctx, ihPeerKey, peer.RawString()).Uint64()
	err = asNil(err)
	if err == nil {
		if deleted == 0 {
			err = storage.ErrResourceDoesNotExist
		} else {
			err = ps.con.Decr(ps.ctx, cntPeerKey).Err()
		}
	}

	return err
}

func (ps *store) PutLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	var ihSummaryKey, ihPeerKey, cntPeerKey string
	log.Debug("storage: PutLeecher", log.Fields{
		"InfoHash": ih,
		"Peer":     peer,
	})
	if peer.Addr().Is6() {
		ihSummaryKey, ihPeerKey, cntPeerKey = ih6Key, ih6LeecherKey, cnt6LeecherKey
	} else {
		ihSummaryKey, ihPeerKey, cntPeerKey = ih4Key, ih4LeecherKey, cnt4LeecherKey
	}
	ihPeerKey += ih.RawString()

	return ps.tx(func(tx redis.Pipeliner) (err error) {
		if err = tx.HSet(ps.ctx, ihPeerKey, peer.RawString(), ps.getClock()).Err(); err != nil {
			return
		}
		if err = tx.Incr(ps.ctx, cntPeerKey).Err(); err != nil {
			return err
		}
		err = tx.SAdd(ps.ctx, ihSummaryKey, ihPeerKey).Err()
		return
	})
}

func (ps *store) DeleteLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	var ihPeerKey, cntPeerKey string
	log.Debug("storage: DeleteLeecher", log.Fields{
		"InfoHash": ih,
		"Peer":     peer,
	})

	if peer.Addr().Is6() {
		ihPeerKey, cntPeerKey = ih6LeecherKey, cnt6LeecherKey
	} else {
		ihPeerKey, cntPeerKey = ih4LeecherKey, cnt4LeecherKey
	}
	ihPeerKey += ih.RawString()

	deleted, err := ps.con.HDel(ps.ctx, ihPeerKey, peer.RawString()).Uint64()
	err = asNil(err)
	if err == nil {
		if deleted == 0 {
			err = storage.ErrResourceDoesNotExist
		} else {
			err = ps.con.Decr(ps.ctx, cntPeerKey).Err()
		}
	}

	return err
}

func (ps *store) GraduateLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	var ihSummaryKey, ihSeederKey, ihLeecherKey, cntSeederKey, cntLeecherKey string
	log.Debug("storage: GraduateLeecher", log.Fields{
		"InfoHash": ih,
		"Peer":     peer,
	})

	if peer.Addr().Is6() {
		ihSummaryKey, ihSeederKey, cntSeederKey = ih6Key, ih6SeederKey, cnt6SeederKey
		ihLeecherKey, cntLeecherKey = ih6LeecherKey, cnt6LeecherKey
	} else {
		ihSummaryKey, ihSeederKey, cntSeederKey = ih4Key, ih4SeederKey, cnt4SeederKey
		ihLeecherKey, cntLeecherKey = ih4LeecherKey, cnt4LeecherKey
	}
	infoHash, peerKey := ih.RawString(), peer.RawString()
	ihSeederKey, ihLeecherKey = ihSeederKey+infoHash, ihLeecherKey+infoHash

	return ps.tx(func(tx redis.Pipeliner) error {
		deleted, err := tx.HDel(ps.ctx, ihLeecherKey, peerKey).Uint64()
		err = asNil(err)
		if err == nil {
			if deleted > 0 {
				err = tx.Decr(ps.ctx, cntLeecherKey).Err()
			}
		}
		if err == nil {
			err = tx.HSet(ps.ctx, ihSeederKey, peerKey, ps.getClock()).Err()
		}
		if err == nil {
			err = tx.Incr(ps.ctx, cntSeederKey).Err()
		}
		if err == nil {
			err = tx.SAdd(ps.ctx, ihSummaryKey, ihSeederKey).Err()
		}
		return err
	})
}

func (ps *store) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, peer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	var ihSeederKey, ihLeecherKey string
	log.Debug("storage: AnnouncePeers", log.Fields{
		"InfoHash": ih,
		"seeder":   seeder,
		"numWant":  numWant,
		"Peer":     peer,
	})

	if peer.Addr().Is6() {
		ihSeederKey, ihLeecherKey = ih6SeederKey, ih6LeecherKey
	} else {
		ihSeederKey, ihLeecherKey = ih4SeederKey, ih4LeecherKey
	}
	infoHash := ih.RawString()
	ihSeederKey, ihLeecherKey = ihSeederKey+infoHash, ihLeecherKey+infoHash

	leechers, err := ps.con.HKeys(ps.ctx, ihLeecherKey).Result()
	err = asNil(err)
	if err != nil {
		return nil, err
	}

	seeders, err := ps.con.HKeys(ps.ctx, ihSeederKey).Result()
	err = asNil(err)
	if err != nil {
		return nil, err
	}

	if len(leechers) == 0 && len(seeders) == 0 {
		return nil, storage.ErrResourceDoesNotExist
	}

	if seeder {
		// Append leechers as possible.
		for _, peerKey := range leechers {
			if numWant == 0 {
				break
			}
			if p, err := bittorrent.NewPeer(peerKey); err == nil {
				peers = append(peers, p)
				numWant--
			} else {
				log.Error("storage: unable to decode leecher", log.Fields{"peer": peerKey})
			}
		}
	} else {
		// Append as many seeders as possible.
		for _, peerKey := range seeders {
			if numWant == 0 {
				break
			}
			if p, err := bittorrent.NewPeer(peerKey); err == nil {
				peers = append(peers, p)
				numWant--
			} else {
				log.Error("storage: unable to decode seeder", log.Fields{"peer": peerKey})
			}
		}

		// Append leechers until we reach numWant.
		if numWant > 0 {
			announcerPK := peer.RawString()
			for _, peerKey := range leechers {
				if peerKey != announcerPK {
					if numWant == 0 {
						break
					}
					if p, err := bittorrent.NewPeer(peerKey); err == nil {
						peers = append(peers, p)
						numWant--
					} else {
						log.Error("storage: unable to decode leecher", log.Fields{"peer": peerKey})
					}
				}
			}
		}
	}

	return
}

func (ps *store) ScrapeSwarm(ih bittorrent.InfoHash, peer bittorrent.Peer) (resp bittorrent.Scrape) {
	var ihSeederKey, ihLeecherKey string
	log.Debug("storage: ScrapeSwarm", log.Fields{
		"InfoHash": ih,
		"Peer":     peer,
	})
	resp.InfoHash = ih
	if peer.Addr().Is6() {
		ihSeederKey, ihLeecherKey = ih6SeederKey, ih6LeecherKey
	} else {
		ihSeederKey, ihLeecherKey = ih4SeederKey, ih4LeecherKey
	}
	infoHash := ih.RawString()
	ihSeederKey, ihLeecherKey = ihSeederKey+infoHash, ihLeecherKey+infoHash

	leechersLen, err := ps.con.HLen(ps.ctx, ihLeecherKey).Result()
	err = asNil(err)
	if err != nil {
		log.Error("storage: Redis HLEN failure", log.Fields{
			"Hkey":  ihLeecherKey,
			"error": err,
		})
		return
	}

	seedersLen, err := ps.con.HLen(ps.ctx, ihSeederKey).Result()
	err = asNil(err)
	if err != nil {
		log.Error("storage: Redis HLEN failure", log.Fields{
			"Hkey":  ihSeederKey,
			"error": err,
		})
		return
	}

	resp.Incomplete = uint32(leechersLen)
	resp.Complete = uint32(seedersLen)

	return
}

func (ps *store) Put(ctx string, value storage.Entry) error {
	return ps.con.HSet(ps.ctx, prefixKey+ctx, value.Key, value.Value).Err()
}

func (ps *store) Contains(ctx string, key string) (bool, error) {
	exist, err := ps.con.HExists(ps.ctx, prefixKey+ctx, key).Result()
	return exist, asNil(err)
}

const argNumErrorMsg = "ERR wrong number of arguments"

func (ps *store) BulkPut(ctx string, pairs ...storage.Entry) (err error) {
	if l := len(pairs); l > 0 {
		args := make([]any, 0, l*2)
		for _, p := range pairs {
			args = append(args, p.Key, p.Value)
		}
		err = ps.con.HSet(ps.ctx, prefixKey+ctx, args...).Err()
		if err != nil {
			if strings.Contains(err.Error(), argNumErrorMsg) {
				log.Warn("This REDIS version/implementation does not support variadic arguments for HSET")
				for _, p := range pairs {
					if err = ps.con.HSet(ps.ctx, prefixKey+ctx, p.Key, p.Value).Err(); err != nil {
						break
					}
				}
			}
		}
	}
	return
}

func (ps *store) Load(ctx string, key string) (v any, err error) {
	v, err = ps.con.HGet(ps.ctx, prefixKey+ctx, key).Result()
	if err != nil && errors.Is(err, redis.Nil) {
		v, err = nil, nil
	}
	return
}

func (ps *store) Delete(ctx string, keys ...string) (err error) {
	if len(keys) > 0 {
		err = asNil(ps.con.HDel(ps.ctx, prefixKey+ctx, keys...).Err())
		if err != nil {
			if strings.Contains(err.Error(), argNumErrorMsg) {
				log.Warn("This REDIS version/implementation does not support variadic arguments for HDEL")
				for _, k := range keys {
					if err = asNil(ps.con.HDel(ps.ctx, prefixKey+ctx, k).Err()); err != nil {
						break
					}
				}
			}
		}
	}
	return
}

func (ps *store) GC(cutoff time.Time) {
	log.Debug("storage: purging peers with no announces since", log.Fields{"before": cutoff})
	cutoffUnix := cutoff.UnixNano()
	ps.gc(cutoffUnix, false)
	ps.gc(cutoffUnix, true)
}

// gc deletes all Peers from the Storage which are older than the
// cutoff time.
//
// This function must be able to execute while other methods on this interface
// are being executed in parallel.
//
// - The Delete(Seeder|Leecher) and GraduateLeecher methods never delete an
//	 infohash key from an addressFamily hash. They also never decrement the
//	 infohash counter.
// - The Put(Seeder|Leecher) and GraduateLeecher methods only ever add infohash
//	 keys to addressFamily hashes and increment the infohash counter.
// - The only method that deletes from the addressFamily hashes is
//	 gc, which also decrements the counters. That means that,
//	 even if a Delete(Seeder|Leecher) call removes the last peer from a swarm,
//	 the infohash counter is not changed and the infohash is left in the
//	 addressFamily hash until it will be cleaned up by gc.
// - gc must run regularly.
// - A WATCH ... MULTI ... EXEC block fails, if between the WATCH and the 'EXEC'
// 	 any of the watched keys have changed. The location of the 'MULTI' doesn't
//	 matter.
//
// We have to analyze four cases to prove our algorithm works. I'll characterize
// them by a tuple (number of peers in a swarm before WATCH, number of peers in
// the swarm during the transaction).
//
// 1. (0,0), the easy case: The swarm is empty, we watch the key, we execute
//	  HLEN and find it empty. We remove it and decrement the counter. It stays
//	  empty the entire time, the transaction goes through.
// 2. (1,n > 0): The swarm is not empty, we watch the key, we find it non-empty,
//	  we unwatch the key. All good. No transaction is made, no transaction fails.
// 3. (0,1): We have to analyze this in two ways.
// - If the change happens before the HLEN call, we will see that the swarm is
//	 not empty and start no transaction.
// - If the change happens after the HLEN, we will attempt a transaction and it
//   will fail. This is okay, the swarm is not empty, we will try cleaning it up
//   next time gc runs.
// 4. (1,0): Again, two ways:
// - If the change happens before the HLEN, we will see an empty swarm. This
//   situation happens if a call to Delete(Seeder|Leecher) removed the last
//	 peer asynchronously. We will attempt a transaction, but the transaction
//	 will fail. This is okay, the infohash key will remain in the addressFamily
//   hash, we will attempt to clean it up the next time 'gc` runs.
// - If the change happens after the HLEN, we will not even attempt to make the
//	 transaction. The infohash key will remain in the addressFamil hash and
//	 we'll attempt to clean it up the next time gc runs.
func (ps *store) gc(cutoffNanos int64, v6 bool) {
	// list all infoHashKeys in the group
	var ihSummaryKey, ihSeederKey, ihLeecherKey, cntSeederKey, cntLeecherKey string
	if v6 {
		cntSeederKey, cntLeecherKey = cnt6SeederKey, cnt6LeecherKey
		ihSummaryKey, ihSeederKey, ihLeecherKey = ih6Key, ih6SeederKey, ih6LeecherKey
	} else {
		cntSeederKey, cntLeecherKey = cnt4SeederKey, cnt4LeecherKey
		ihSummaryKey, ihSeederKey, ihLeecherKey = ih4Key, ih4SeederKey, ih4LeecherKey
	}
	infoHashKeys, err := ps.con.SMembers(ps.ctx, ihSummaryKey).Result()
	err = asNil(err)
	if err == nil {
		for _, infoHashKey := range infoHashKeys {
			var cntKey string
			var seeder bool
			if seeder = strings.HasPrefix(infoHashKey, ihSeederKey); seeder {
				cntKey = cntSeederKey
			} else if strings.HasPrefix(infoHashKey, ihLeecherKey) {
				cntKey = cntLeecherKey
			} else {
				log.Warn("storage: Redis: unexpected record found in info hash set", log.Fields{
					"hashSet":     ihSummaryKey,
					"infoHashKey": infoHashKey,
				})
				continue
			}
			// list all (peer, timeout) pairs for the ih
			peerList, err := ps.con.HGetAll(ps.ctx, infoHashKey).Result()
			err = asNil(err)
			if err == nil {
				var removedPeerCount int64
				for peerKey, timeStamp := range peerList {
					var peer bittorrent.Peer
					if peer, err = bittorrent.NewPeer(peerKey); err == nil {
						if mtime, err := strconv.ParseInt(timeStamp, 10, 64); err == nil {
							if mtime <= cutoffNanos {
								log.Debug("storage: Redis: deleting peer", log.Fields{
									"Peer": peer,
								})
								var count int64
								count, err = ps.con.HDel(ps.ctx, infoHashKey, peerKey).Result()
								err = asNil(err)
								if err == nil {
									removedPeerCount += count
								}
							}
						}
					}
					if err != nil {
						log.Error("storage: Redis: unable to delete info hash peer", log.Fields{
							"hashSet":     ihSummaryKey,
							"infoHashKey": infoHashKey,
							"peer":        peer,
							"key":         peerKey,
							"error":       err,
						})
					}
				}
				// DECR seeder/leecher counter
				if removedPeerCount > 0 {
					if err := ps.con.DecrBy(ps.ctx, cntKey, removedPeerCount).Err(); err != nil {
						log.Error("storage: Redis: unable to decrement seeder/leecher peer count", log.Fields{
							"hashSet":     ihSummaryKey,
							"infoHashKey": infoHashKey,
							"key":         cntKey,
							"error":       err,
						})
					}
				}

				err = asNil(ps.con.Watch(ps.ctx, func(tx *redis.Tx) (err error) {
					var infoHashCount uint64
					infoHashCount, err = ps.con.HLen(ps.ctx, infoHashKey).Uint64()
					err = asNil(err)
					if err == nil && infoHashCount == 0 {
						// Empty hashes are not shown among existing keys,
						// in other words, it's removed automatically after `HDEL` the last field.
						// _, err := ps.con.Del(ps.ctx, infoHashKey)
						err = asNil(ps.con.SRem(ps.ctx, ihSummaryKey, infoHashKey).Err())
					}
					return err
				}, infoHashKey))
				if err != nil {
					log.Error("storage: Redis: unable to clean info hash records", log.Fields{
						"hashSet":     ihSummaryKey,
						"infoHashKey": infoHashKey,
						"error":       err,
					})
				}
			} else {
				log.Error("storage: Redis: unable to fetch info hash peers", log.Fields{
					"hashSet":     ihSummaryKey,
					"infoHashKey": infoHashKey,
					"error":       err,
				})
			}
		}
	} else {
		log.Error("storage: Redis: unable to fetch info hash set", log.Fields{"hashSet": ihSummaryKey, "error": err})
	}
}

func (ps *store) Stop() stop.Result {
	c := make(stop.Channel)
	go func() {
		if ps.closed != nil {
			close(ps.closed)
		}
		ps.wg.Wait()
		var err error
		if ps.con != nil {
			log.Info("storage: exiting. mochi does not clear data in redis when exiting. mochi keys have prefix 'IPv{4,6}_'.")
			err = ps.con.Close()
		}
		c.Done(err)
	}()

	return c.Result()
}

func (ps *store) LogFields() log.Fields {
	fields := make(log.Fields, len(ps.logFields))
	for k, v := range ps.logFields {
		fields[k] = v
	}
	return fields
}
