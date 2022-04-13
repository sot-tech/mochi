// Package redis implements the storage interface for a Conf
// BitTorrent tracker keeping peer data in redis with hash.
// There two categories of hash:
//
// - IPv{4,6}_{L,S}_infohash
//	To save peers that hold the infohash, used for fast searching,
//  deleting, and timeout handling
//
// - IPv{4,6}
//  To save all the infohashes, used for garbage collection,
//	metrics aggregation and leecher graduation
//
// Tree keys are used to record the count of swarms, seeders
// and leechers for each group (IPv4, IPv6).
//
// - IPv{4,6}_infohash_count
//	To record the number of infohashes.
//
// - IPv{4,6}_S_count
//	To record the number of seeders.
//
// - IPv{4,6}_L_count
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
	"gopkg.in/yaml.v3"

	"github.com/sot-tech/mochi/bittorrent"
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
)

// ErrSentinelAndClusterChecked returned from initializer if both Config.Sentinel and Config.Cluster provided
var ErrSentinelAndClusterChecked = errors.New("unable to use both cluster and sentinel mode")

func init() {
	// Register the storage driver.
	storage.RegisterDriver(Name, driver{})
}

type driver struct{}

func (d driver) NewStorage(icfg any) (storage.Storage, error) {
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

// Config holds the configuration of a redis Storage.
type Config struct {
	GarbageCollectionInterval   time.Duration `yaml:"gc_interval"`
	PrometheusReportingInterval time.Duration `yaml:"prometheus_reporting_interval"`
	PeerLifetime                time.Duration `yaml:"peer_lifetime"`
	Addresses                   []string      `yaml:"addresses"`
	Login                       string        `yaml:"login"`
	Password                    string        `yaml:"password"`
	Sentinel                    bool          `yaml:"sentinel"`
	SentinelMaster              string        `yaml:"sentinel_master"`
	Cluster                     bool          `yaml:"cluster"`
	DB                          int           `yaml:"db"`
	PoolSize                    int           `yaml:"pool_size"`
	ReadTimeout                 time.Duration `yaml:"read_timeout"`
	WriteTimeout                time.Duration `yaml:"write_timeout"`
	ConnectTimeout              time.Duration `yaml:"connect_timeout"`
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
	go ps.runGC(cfg.GarbageCollectionInterval, cfg.PeerLifetime)

	if cfg.PrometheusReportingInterval > 0 {
		// Start a goroutine for reporting statistics to Prometheus.
		ps.wg.Add(1)
		go ps.runProm(cfg.PrometheusReportingInterval)
	} else {
		log.Info("prometheus disabled because of zero reporting interval")
	}

	return ps, nil
}

func (ps *store) runGC(gcInterval, peerLifeTime time.Duration) {
	defer ps.wg.Done()
	for {
		select {
		case <-ps.closed:
			return
		case <-time.After(gcInterval):
			before := time.Now().Add(-peerLifeTime)
			log.Debug("storage: purging peers with no announces since", log.Fields{"before": before})
			ps.collectGarbage(before)
		}
	}
}

func (ps *store) runProm(reportInterval time.Duration) {
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

var groups = []string{bittorrent.IPv4.String(), bittorrent.IPv6.String()}

// leecherInfoHashKey generates string IPvN_L_hash
func leecherInfoHashKey(addressFamily, infoHash string) string {
	return addressFamily + "_L_" + infoHash
}

// seederInfoHashKey generates string IPvN_S_hash
func seederInfoHashKey(addressFamily, infoHash string) string {
	return addressFamily + "_S_" + infoHash
}

// seederInfoHashKey generates string IPvN_infohash_count
func infoHashCountKey(addressFamily string) string {
	return addressFamily + "_infohash_count"
}

// seederInfoHashKey generates string IPvN_L_count
func leecherCountKey(addressFamily string) string {
	return addressFamily + "_L_count"
}

// seederInfoHashKey generates string IPvN_S_count
func seederCountKey(addressFamily string) string {
	return addressFamily + "_S_count"
}

// populateProm aggregates metrics over all groups and then posts them to
// prometheus.
func (ps *store) populateProm() {
	var numInfoHashes, numSeeders, numLeechers int64

	for _, group := range groups {
		if n, err := ps.con.Get(ps.ctx, infoHashCountKey(group)).Int64(); err != nil && !errors.Is(err, redis.Nil) {
			log.Error("storage: GET counter failure", log.Fields{
				"key":   infoHashCountKey(group),
				"error": err,
			})
		} else {
			numInfoHashes += n
		}
		if n, err := ps.con.Get(ps.ctx, seederCountKey(group)).Int64(); err != nil && !errors.Is(err, redis.Nil) {
			log.Error("storage: GET counter failure", log.Fields{
				"key":   seederCountKey(group),
				"error": err,
			})
		} else {
			numSeeders += n
		}
		if n, err := ps.con.Get(ps.ctx, leecherCountKey(group)).Int64(); err != nil && !errors.Is(err, redis.Nil) {
			log.Error("storage: GET counter failure", log.Fields{
				"key":   leecherCountKey(group),
				"error": err,
			})
		} else {
			numLeechers += n
		}
	}

	storage.PromInfoHashesCount.Set(float64(numInfoHashes))
	storage.PromSeedersCount.Set(float64(numSeeders))
	storage.PromLeechersCount.Set(float64(numLeechers))
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

func (ps *store) PutSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	addressFamily := p.IP.AddressFamily.String()
	log.Debug("storage: PutSeeder", log.Fields{
		"InfoHash": ih,
		"Peer":     p,
	})

	encodedSeederInfoHash := seederInfoHashKey(addressFamily, ih.RawString())
	now := ps.getClock()

	return ps.tx(func(tx redis.Pipeliner) (err error) {
		if err = tx.HSet(ps.ctx, encodedSeederInfoHash, p.RawString(), now).Err(); err != nil {
			return
		}
		if err = ps.con.Incr(ps.ctx, seederCountKey(addressFamily)).Err(); err != nil {
			return
		}
		if err = ps.con.HSet(ps.ctx, addressFamily, encodedSeederInfoHash, now).Err(); err != nil {
			return
		}
		err = ps.con.Incr(ps.ctx, infoHashCountKey(addressFamily)).Err()
		return
	})
}

func (ps *store) DeleteSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	addressFamily := p.IP.AddressFamily.String()
	log.Debug("storage: DeleteSeeder", log.Fields{
		"InfoHash": ih,
		"Peer":     p,
	})

	encodedSeederInfoHash := seederInfoHashKey(addressFamily, ih.RawString())
	deleted, err := ps.con.HDel(ps.ctx, encodedSeederInfoHash, p.RawString()).Uint64()
	err = asNil(err)
	if err == nil {
		if deleted == 0 {
			err = storage.ErrResourceDoesNotExist
		} else {
			err = ps.con.Decr(ps.ctx, seederCountKey(addressFamily)).Err()
		}
	}

	return err
}

func (ps *store) PutLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	addressFamily := p.IP.AddressFamily.String()
	log.Debug("storage: PutLeecher", log.Fields{
		"InfoHash": ih,
		"Peer":     p,
	})

	// Update the peer in the swarm.
	encodedLeecherInfoHash := leecherInfoHashKey(addressFamily, ih.RawString())
	now := ps.getClock()

	return ps.tx(func(tx redis.Pipeliner) (err error) {
		if err = tx.HSet(ps.ctx, encodedLeecherInfoHash, p.RawString(), now).Err(); err != nil {
			return
		}
		if err = tx.HSet(ps.ctx, addressFamily, encodedLeecherInfoHash, now).Err(); err != nil {
			return err
		}
		err = tx.Incr(ps.ctx, leecherCountKey(addressFamily)).Err()
		return
	})
}

func (ps *store) DeleteLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	addressFamily := p.IP.AddressFamily.String()
	log.Debug("storage: DeleteLeecher", log.Fields{
		"InfoHash": ih,
		"Peer":     p,
	})

	encodedLeecherInfoHash := leecherInfoHashKey(addressFamily, ih.RawString())

	deleted, err := ps.con.HDel(ps.ctx, encodedLeecherInfoHash, p.RawString()).Uint64()
	err = asNil(err)
	if err == nil {
		if deleted == 0 {
			err = storage.ErrResourceDoesNotExist
		} else {
			err = ps.con.Decr(ps.ctx, leecherCountKey(addressFamily)).Err()
		}
	}

	return err
}

func (ps *store) GraduateLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	addressFamily := p.IP.AddressFamily.String()
	log.Debug("storage: GraduateLeecher", log.Fields{
		"InfoHash": ih,
		"Peer":     p,
	})

	encodedInfoHash := ih.RawString()
	encodedLeecherInfoHash := leecherInfoHashKey(addressFamily, encodedInfoHash)
	encodedSeederInfoHash := seederInfoHashKey(addressFamily, encodedInfoHash)
	peerKey := p.RawString()
	now := ps.getClock()

	return ps.tx(func(tx redis.Pipeliner) error {
		deleted, err := tx.HDel(ps.ctx, encodedLeecherInfoHash, peerKey).Uint64()
		err = asNil(err)
		if err == nil {
			if deleted > 0 {
				err = tx.Decr(ps.ctx, leecherCountKey(addressFamily)).Err()
			}
		}
		if err == nil {
			err = tx.HSet(ps.ctx, encodedSeederInfoHash, peerKey, now).Err()
		}
		if err == nil {
			err = tx.Incr(ps.ctx, seederCountKey(addressFamily)).Err()
		}
		if err == nil {
			err = tx.HSet(ps.ctx, addressFamily, encodedSeederInfoHash, now).Err()
		}
		if err == nil {
			err = tx.Incr(ps.ctx, infoHashCountKey(addressFamily)).Err()
		}
		return err
	})
}

func (ps *store) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, announcer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	addressFamily := announcer.IP.AddressFamily.String()
	log.Debug("storage: AnnouncePeers", log.Fields{
		"InfoHash": ih,
		"seeder":   seeder,
		"numWant":  numWant,
		"Peer":     announcer,
	})

	encodedInfoHash := ih.RawString()
	encodedLeecherInfoHash := leecherInfoHashKey(addressFamily, encodedInfoHash)
	encodedSeederInfoHash := seederInfoHashKey(addressFamily, encodedInfoHash)

	leechers, err := ps.con.HKeys(ps.ctx, encodedLeecherInfoHash).Result()
	err = asNil(err)
	if err != nil {
		return nil, err
	}

	seeders, err := ps.con.HKeys(ps.ctx, encodedSeederInfoHash).Result()
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
			announcerPK := announcer.RawString()
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

func (ps *store) ScrapeSwarm(ih bittorrent.InfoHash, af bittorrent.AddressFamily) (resp bittorrent.Scrape) {
	resp.InfoHash = ih
	addressFamily := af.String()
	encodedInfoHash := ih.RawString()
	encodedLeecherInfoHash := leecherInfoHashKey(addressFamily, encodedInfoHash)
	encodedSeederInfoHash := seederInfoHashKey(addressFamily, encodedInfoHash)

	leechersLen, err := ps.con.HLen(ps.ctx, encodedLeecherInfoHash).Result()
	err = asNil(err)
	if err != nil {
		log.Error("storage: Redis HLEN failure", log.Fields{
			"Hkey":  encodedLeecherInfoHash,
			"error": err,
		})
		return
	}

	seedersLen, err := ps.con.HLen(ps.ctx, encodedSeederInfoHash).Result()
	err = asNil(err)
	if err != nil {
		log.Error("storage: Redis HLEN failure", log.Fields{
			"Hkey":  encodedSeederInfoHash,
			"error": err,
		})
		return
	}

	resp.Incomplete = uint32(leechersLen)
	resp.Complete = uint32(seedersLen)

	return
}

func (ps *store) Put(ctx string, value storage.Entry) error {
	return ps.con.HSet(ps.ctx, ctx, value.Key, value.Value).Err()
}

func (ps *store) Contains(ctx string, key string) (bool, error) {
	exist, err := ps.con.HExists(ps.ctx, ctx, key).Result()
	return exist, asNil(err)
}

const argNumErrorMsg = "ERR wrong number of arguments"

func (ps *store) BulkPut(ctx string, pairs ...storage.Entry) (err error) {
	if l := len(pairs); l > 0 {
		args := make([]any, 0, l*2)
		for _, p := range pairs {
			args = append(args, p.Key, p.Value)
		}
		err = ps.con.HSet(ps.ctx, ctx, args...).Err()
		if err != nil {
			if strings.Contains(err.Error(), argNumErrorMsg) {
				log.Warn("This REDIS version/implementation does not support variadic arguments for HSET")
				for _, p := range pairs {
					if err = ps.con.HSet(ps.ctx, ctx, p.Key, p.Value).Err(); err != nil {
						break
					}
				}
			}
		}
	}
	return
}

func (ps *store) Load(ctx string, key string) (v any, err error) {
	v, err = ps.con.HGet(ps.ctx, ctx, key).Result()
	if err != nil && errors.Is(err, redis.Nil) {
		v, err = nil, nil
	}
	return
}

func (ps *store) Delete(ctx string, keys ...string) (err error) {
	if len(keys) > 0 {
		err = asNil(ps.con.HDel(ps.ctx, ctx, keys...).Err())
		if err != nil {
			if strings.Contains(err.Error(), argNumErrorMsg) {
				log.Warn("This REDIS version/implementation does not support variadic arguments for HDEL")
				for _, k := range keys {
					if err = asNil(ps.con.HDel(ps.ctx, ctx, k).Err()); err != nil {
						break
					}
				}
			}
		}
	}
	return
}

// collectGarbage deletes all Peers from the Storage which are older than the
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
//	 collectGarbage, which also decrements the counters. That means that,
//	 even if a Delete(Seeder|Leecher) call removes the last peer from a swarm,
//	 the infohash counter is not changed and the infohash is left in the
//	 addressFamily hash until it will be cleaned up by collectGarbage.
// - collectGarbage must run regularly.
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
//   next time collectGarbage runs.
// 4. (1,0): Again, two ways:
// - If the change happens before the HLEN, we will see an empty swarm. This
//   situation happens if a call to Delete(Seeder|Leecher) removed the last
//	 peer asynchronously. We will attempt a transaction, but the transaction
//	 will fail. This is okay, the infohash key will remain in the addressFamily
//   hash, we will attempt to clean it up the next time 'collectGarbage` runs.
// - If the change happens after the HLEN, we will not even attempt to make the
//	 transaction. The infohash key will remain in the addressFamil hash and
//	 we'll attempt to clean it up the next time collectGarbage runs.
func (ps *store) collectGarbage(cutoff time.Time) {
	cutoffUnix := cutoff.UnixNano()
	start := time.Now()
	var err error
	for _, group := range groups {
		// list all infoHashes in the group
		var infoHashes []string
		infoHashes, err = ps.con.HKeys(ps.ctx, group).Result()
		err = asNil(err)
		if err == nil {
			for _, infoHash := range infoHashes {
				isSeeder := len(infoHash) > 5 && infoHash[5:6] == "S"
				// list all (peer, timeout) pairs for the ih
				peerList, err := ps.con.HGetAll(ps.ctx, infoHash).Result()
				err = asNil(err)
				if err == nil {
					var removedPeerCount int64
					for peerKey, timeStamp := range peerList {
						var peer bittorrent.Peer
						if peer, err = bittorrent.NewPeer(peerKey); err == nil {
							if mtime, err := strconv.ParseInt(timeStamp, 10, 64); err == nil {
								if mtime <= cutoffUnix {
									log.Debug("storage: deleting peer", log.Fields{
										"Peer": peer,
									})
									var count int64
									count, err = ps.con.HDel(ps.ctx, infoHash, peerKey).Result()
									err = asNil(err)
									if err == nil {
										removedPeerCount += count
									}
								}
							}
						}
						if err != nil {
							log.Error("storage: Redis: unable to delete info hash peer", log.Fields{
								"group":    group,
								"infoHash": infoHash,
								"peer":     peer,
								"key":      peerKey,
								"error":    err,
							})
						}
					}
					// DECR seeder/leecher counter
					if removedPeerCount > 0 {
						var decrCounter string
						if isSeeder {
							decrCounter = seederCountKey(group)
						} else {
							decrCounter = leecherCountKey(group)
						}
						if err := ps.con.DecrBy(ps.ctx, decrCounter, removedPeerCount).Err(); err != nil {
							log.Error("storage: Redis: unable to decrement seeder/leecher peer count", log.Fields{
								"group":    group,
								"infoHash": infoHash,
								"key":      decrCounter,
								"error":    err,
							})
						}
					}

					// use WATCH to avoid race condition
					// https://redis.io/topics/transactions
					err = asNil(ps.con.Watch(ps.ctx, func(tx *redis.Tx) (err error) {
						var infoHashCount int64
						infoHashCount, err = ps.con.HLen(ps.ctx, infoHash).Result()
						err = asNil(err)
						if err == nil && infoHashCount == 0 {
							// Empty hashes are not shown among existing keys,
							// in other words, it's removed automatically after `HDEL` the last field.
							// _, err := ps.con.Del(ps.ctx, infoHash)
							var deletedCount int64
							deletedCount, err = ps.con.HDel(ps.ctx, group, infoHash).Result()
							err = asNil(err)
							if err == nil && isSeeder && deletedCount > 0 {
								err = ps.con.Decr(ps.ctx, infoHashCountKey(group)).Err()
							}
						}
						return err
					}, infoHash))
					if err != nil {
						log.Error("storage: Redis: unable to clean info hash records", log.Fields{
							"group":    group,
							"infoHash": infoHash,
							"error":    err,
						})
					}
				} else {
					log.Error("storage: Redis: unable to fetch info hash peers", log.Fields{
						"group":    group,
						"infoHash": infoHash,
						"error":    err,
					})
				}
			}
		} else {
			log.Error("storage: Redis: unable to fetch info hashes", log.Fields{"group": group, "error": err})
		}
	}

	duration := time.Since(start).Milliseconds()
	log.Debug("storage: recordGCDuration", log.Fields{"timeTaken(ms)": duration})
	storage.PromGCDurationMilliseconds.Observe(float64(duration))
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
