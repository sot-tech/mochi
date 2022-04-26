// Package redis implements the storage interface.
// BitTorrent tracker keeping peer data in redis with hash.
// There two categories of hash:
//
// - CHI_{L,S}{4,6}_<HASH> (hash type)
//	To save peers that hold the infohash, used for fast searching,
//  deleting, and timeout handling
//
// - CHI_I (set type)
//  To save all the infohashes, used for garbage collection,
//	metrics aggregation and leecher graduation
//
// Two keys are used to record the count of seeders and leechers.
//
// - CHI_C_S (key type)
//	To record the number of seeders.
//
// - CHI_C_L (key type)
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
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

const (
	// Name is the name by which this peer store is registered with Conf.
	Name = "redis"
	// Default config constants.
	defaultRedisAddress   = "127.0.0.1:6379"
	defaultReadTimeout    = time.Second * 15
	defaultWriteTimeout   = time.Second * 15
	defaultConnectTimeout = time.Second * 15
	// PrefixKey prefix which will be prepended to ctx argument in storage.DataStorage calls
	PrefixKey = "CHI_"
	// IHKey redis hash key for all info hashes
	IHKey = "CHI_I"
	// IH4SeederKey redis hash key prefix for IPv4 seeders
	IH4SeederKey = "CHI_S4_"
	// IH6SeederKey redis hash key prefix for IPv6 seeders
	IH6SeederKey = "CHI_S6_"
	// IH4LeecherKey redis hash key prefix for IPv4 leechers
	IH4LeecherKey = "CHI_L4_"
	// IH6LeecherKey redis hash key prefix for IPv6 leechers
	IH6LeecherKey = "CHI_L6_"
	// CountSeederKey redis key for seeder count
	CountSeederKey = "CHI_C_S"
	// CountLeecherKey redis key for leecher count
	CountLeecherKey = "CHI_C_L"
)

// ErrSentinelAndClusterChecked returned from initializer if both Config.Sentinel and Config.Cluster provided
var ErrSentinelAndClusterChecked = errors.New("unable to use both cluster and sentinel mode")

func init() {
	// Register the storage builder.
	storage.RegisterBuilder(Name, builder)
}

func builder(icfg conf.MapConfig) (storage.PeerStorage, error) {
	// Unmarshal the bytes into the proper config type.
	var cfg Config

	if err := icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return newStore(cfg)
}

func newStore(cfg Config) (*store, error) {
	cfg, err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	rs, err := cfg.Connect()
	if err != nil {
		return nil, err
	}

	return &store{
		Connection: rs,
		closed:     make(chan any),
		wg:         sync.WaitGroup{},
		logFields:  cfg.LogFields(),
	}, nil
}

// Config holds the configuration of a redis PeerStorage.
type Config struct {
	PeerLifetime   time.Duration `cfg:"peer_lifetime"`
	Addresses      []string
	DB             int
	PoolSize       int `cfg:"pool_size"`
	Login          string
	Password       string
	Sentinel       bool
	SentinelMaster string `cfg:"sentinel_master"`
	Cluster        bool
	ReadTimeout    time.Duration `cfg:"read_timeout"`
	WriteTimeout   time.Duration `cfg:"write_timeout"`
	ConnectTimeout time.Duration `cfg:"connect_timeout"`
}

// LogFields renders the current config as a set of Logrus fields.
func (cfg Config) LogFields() log.Fields {
	return log.Fields{
		"name":           Name,
		"peerLifetime":   cfg.PeerLifetime,
		"addresses":      cfg.Addresses,
		"db":             cfg.DB,
		"poolSize":       cfg.PoolSize,
		"sentinel":       cfg.Sentinel,
		"sentinelMaster": cfg.SentinelMaster,
		"cluster":        cfg.Cluster,
		"readTimeout":    cfg.ReadTimeout,
		"writeTimeout":   cfg.WriteTimeout,
		"connectTimeout": cfg.ConnectTimeout,
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

	return validCfg, nil
}

// Connect creates redis client from configuration
func (cfg Config) Connect() (con Connection, err error) {
	var rs redis.UniversalClient
	switch {
	case cfg.Cluster:
		rs = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.Addresses,
			Username:     cfg.Login,
			Password:     cfg.Password,
			DialTimeout:  cfg.ConnectTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			PoolSize:     cfg.PoolSize,
		})
	case cfg.Sentinel:
		rs = redis.NewFailoverClient(&redis.FailoverOptions{
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
		rs = redis.NewClient(&redis.Options{
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
	if err = rs.Ping(context.Background()).Err(); err == nil && !errors.Is(err, redis.Nil) {
		err = nil
	} else {
		_ = rs.Close()
		rs = nil
	}
	return Connection{rs}, err
}

func (ps *store) ScheduleGC(gcInterval, peerLifeTime time.Duration) {
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
				start := time.Now()
				ps.gc(time.Now().Add(-peerLifeTime))
				duration := time.Since(start).Milliseconds()
				log.Debug("storage: Redis: recordGCDuration", log.Fields{"timeTaken(ms)": duration})
				storage.PromGCDurationMilliseconds.Observe(float64(duration))
				t.Reset(gcInterval)
			}
		}
	}()
}

func (ps *store) ScheduleStatisticsCollection(reportInterval time.Duration) {
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
					// populateProm aggregates metrics over all groups and then posts them to
					// prometheus.
					numInfoHashes := ps.count(IHKey, true)
					numSeeders := ps.count(CountSeederKey, false)
					numLeechers := ps.count(CountLeecherKey, false)

					storage.PromInfoHashesCount.Set(float64(numInfoHashes))
					storage.PromSeedersCount.Set(float64(numSeeders))
					storage.PromLeechersCount.Set(float64(numLeechers))
					log.Debug("storage: Redis: populateProm() finished", log.Fields{"timeTaken": time.Since(before)})
				}
			}
		}
	}()
}

// Connection is wrapper for redis.UniversalClient
type Connection struct {
	redis.UniversalClient
}

type store struct {
	Connection
	closed    chan any
	wg        sync.WaitGroup
	logFields log.Fields
}

func (ps *store) count(key string, getLength bool) (n uint64) {
	var err error
	if getLength {
		n, err = ps.SCard(context.Background(), key).Uint64()
	} else {
		n, err = ps.Get(context.Background(), key).Uint64()
	}
	err = AsNil(err)
	if err != nil {
		log.Error("storage: Redis: GET/SCARD failure", log.Fields{
			"key":   key,
			"error": err,
		})
	}
	return
}

func (ps *store) getClock() int64 {
	return timecache.NowUnixNano()
}

func (ps *store) tx(txf func(tx redis.Pipeliner) error) (err error) {
	if pipe, txErr := ps.TxPipelined(context.TODO(), txf); txErr == nil {
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

// AsNil returns nil if provided err is redis.Nil
// otherwise returns err
func AsNil(err error) error {
	if err == nil || errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

// InfoHashKey generates redis key for provided hash and flags
func InfoHashKey(infoHash string, seeder, v6 bool) (infoHashKey string) {
	var bm int
	if seeder {
		bm = 0b01
	}
	if v6 {
		bm |= 0b10
	}
	switch bm {
	case 0b11:
		infoHashKey = IH6SeederKey
	case 0b10:
		infoHashKey = IH6LeecherKey
	case 0b01:
		infoHashKey = IH4SeederKey
	case 0b00:
		infoHashKey = IH4LeecherKey
	}
	infoHashKey += infoHash
	return
}

func (ps *store) putPeer(infoHashKey, peerCountKey, peerID string) error {
	log.Debug("storage: Redis: PutPeer", log.Fields{
		"infoHashKey":  infoHashKey,
		"peerCountKey": peerCountKey,
		"peerID":       peerID,
	})
	return ps.tx(func(tx redis.Pipeliner) (err error) {
		if err = tx.HSet(context.TODO(), infoHashKey, peerID, ps.getClock()).Err(); err != nil {
			return
		}
		if err = tx.Incr(context.TODO(), peerCountKey).Err(); err != nil {
			return
		}
		err = tx.SAdd(context.TODO(), IHKey, infoHashKey).Err()
		return
	})
}

func (ps *store) delPeer(infoHashKey, peerCountKey, peerID string) error {
	log.Debug("storage: Redis: DeletePeer", log.Fields{
		"infoHashKey":  infoHashKey,
		"peerCountKey": peerCountKey,
		"peerID":       peerID,
	})
	deleted, err := ps.HDel(context.TODO(), infoHashKey, peerID).Uint64()
	err = AsNil(err)
	if err == nil {
		if deleted == 0 {
			err = storage.ErrResourceDoesNotExist
		} else {
			err = ps.Decr(context.TODO(), peerCountKey).Err()
		}
	}

	return err
}

func (ps *store) PutSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return ps.putPeer(InfoHashKey(ih.RawString(), true, peer.Addr().Is6()), CountSeederKey, peer.RawString())
}

func (ps *store) DeleteSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return ps.delPeer(InfoHashKey(ih.RawString(), true, peer.Addr().Is6()), CountSeederKey, peer.RawString())
}

func (ps *store) PutLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return ps.putPeer(InfoHashKey(ih.RawString(), false, peer.Addr().Is6()), CountLeecherKey, peer.RawString())
}

func (ps *store) DeleteLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return ps.delPeer(InfoHashKey(ih.RawString(), false, peer.Addr().Is6()), CountLeecherKey, peer.RawString())
}

func (ps *store) GraduateLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	log.Debug("storage: Redis: GraduateLeecher", log.Fields{
		"infoHash": ih,
		"peer":     peer,
	})

	infoHash, peerID, isV6 := ih.RawString(), peer.RawString(), peer.Addr().Is6()
	ihSeederKey, ihLeecherKey := InfoHashKey(infoHash, true, isV6), InfoHashKey(infoHash, false, isV6)

	return ps.tx(func(tx redis.Pipeliner) error {
		deleted, err := tx.HDel(context.TODO(), ihLeecherKey, peerID).Uint64()
		err = AsNil(err)
		if err == nil {
			if deleted > 0 {
				err = tx.Decr(context.TODO(), CountLeecherKey).Err()
			}
		}
		if err == nil {
			err = tx.HSet(context.TODO(), ihSeederKey, peerID, ps.getClock()).Err()
		}
		if err == nil {
			err = tx.Incr(context.TODO(), CountSeederKey).Err()
		}
		if err == nil {
			err = tx.SAdd(context.TODO(), IHKey, ihSeederKey).Err()
		}
		return err
	})
}

func (ps Connection) parsePeersList(peersResult *redis.StringSliceCmd) (peers []bittorrent.Peer, err error) {
	var peerIds []string
	peerIds, err = peersResult.Result()
	if err = AsNil(err); err == nil {
		for _, peerID := range peerIds {
			if p, err := bittorrent.NewPeer(peerID); err == nil {
				peers = append(peers, p)
			} else {
				log.Error("storage: Redis: unable to decode leecher", log.Fields{"peerID": peerID})
			}
		}
	}
	return
}

type getPeersFn func(context.Context, string, int) *redis.StringSliceCmd

// GetPeers retrieves peers for provided info hash by calling membersFn and
// converts result to bittorrent.Peer array.
// If forSeeder set to true - returns only leechers, if false -
// seeders and if maxCount not reached - leechers.
func (ps Connection) GetPeers(ih bittorrent.InfoHash, forSeeder bool, maxCount int, isV6 bool, membersFn getPeersFn) (out []bittorrent.Peer, err error) {
	infoHash := ih.RawString()

	infoHashKeys := make([]string, 1, 2)

	if forSeeder {
		infoHashKeys[0] = InfoHashKey(infoHash, false, isV6)
	} else {
		infoHashKeys[0] = InfoHashKey(infoHash, true, isV6)
		infoHashKeys = append(infoHashKeys, InfoHashKey(infoHash, false, isV6))
	}

	for _, infoHashKey := range infoHashKeys {
		var peers []bittorrent.Peer
		peers, err = ps.parsePeersList(membersFn(context.TODO(), infoHashKey, maxCount))
		maxCount -= len(peers)
		out = append(out, peers...)
		if err != nil || maxCount <= 0 {
			break
		}
	}

	if l := len(out); err == nil {
		if l == 0 {
			err = storage.ErrResourceDoesNotExist
		}
	} else if l > 0 {
		err = nil
		log.Warn("storage: Redis: error occurred while retrieving peers", log.Fields{
			"infoHash": infoHash,
			"error":    err,
		})
	}

	return
}

func (ps *store) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, v6 bool) ([]bittorrent.Peer, error) {
	log.Debug("storage: Redis: AnnouncePeers", log.Fields{
		"infoHash": ih,
		"seeder":   seeder,
		"numWant":  numWant,
		"peer":     v6,
	})

	return ps.GetPeers(ih, seeder, numWant, v6, func(ctx context.Context, infoHashKey string, maxCount int) *redis.StringSliceCmd {
		return ps.HRandField(ctx, infoHashKey, maxCount, false)
	})
}

type getPeerCountFn func(context.Context, string) *redis.IntCmd

func (ps Connection) countPeers(infoHashKey string, countFn getPeerCountFn) uint32 {
	count, err := countFn(context.TODO(), infoHashKey).Result()
	err = AsNil(err)
	if err != nil {
		log.Error("storage: Redis: key size calculation failure", log.Fields{
			"infoHashKey": infoHashKey,
			"error":       err,
		})
	}
	return uint32(count)
}

// CountPeers calls provided countFn and returns seeders and leechers count for specified info hash
func (ps Connection) CountPeers(ih bittorrent.InfoHash, countFn getPeerCountFn) (leechersCount, seedersCount uint32) {
	infoHash := ih.RawString()

	leechersCount = ps.countPeers(InfoHashKey(infoHash, false, false), countFn) +
		ps.countPeers(InfoHashKey(infoHash, false, true), countFn)
	seedersCount = ps.countPeers(InfoHashKey(infoHash, true, false), countFn) +
		ps.countPeers(InfoHashKey(infoHash, true, true), countFn)

	return
}

func (ps *store) ScrapeSwarm(ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32) {
	log.Debug("storage: Redis ScrapeSwarm", log.Fields{
		"infoHash": ih,
	})

	leechers, seeders = ps.CountPeers(ih, ps.HLen)

	return
}

const argNumErrorMsg = "ERR wrong number of arguments"

// Put - storage.DataStorage implementation
func (ps Connection) Put(ctx string, values ...storage.Entry) (err error) {
	if l := len(values); l > 0 {
		if l == 1 {
			err = ps.HSet(context.TODO(), PrefixKey+ctx, values[0].Key, values[0].Value).Err()
		} else {
			args := make([]any, 0, l*2)
			for _, p := range values {
				args = append(args, p.Key, p.Value)
			}
			err = ps.HSet(context.TODO(), PrefixKey+ctx, args...).Err()
			if err != nil {
				if strings.Contains(err.Error(), argNumErrorMsg) {
					log.Warn("This Redis version/implementation does not support variadic arguments for HSET")
					for _, p := range values {
						if err = ps.HSet(context.TODO(), PrefixKey+ctx, p.Key, p.Value).Err(); err != nil {
							break
						}
					}
				}
			}
		}
	}
	return
}

// Contains - storage.DataStorage implementation
func (ps Connection) Contains(ctx string, key string) (bool, error) {
	exist, err := ps.HExists(context.TODO(), PrefixKey+ctx, key).Result()
	return exist, AsNil(err)
}

// Load - storage.DataStorage implementation
func (ps Connection) Load(ctx string, key string) (v any, err error) {
	v, err = ps.HGet(context.TODO(), PrefixKey+ctx, key).Result()
	if err != nil && errors.Is(err, redis.Nil) {
		v, err = nil, nil
	}
	return
}

// Delete - storage.DataStorage implementation
func (ps Connection) Delete(ctx string, keys ...string) (err error) {
	if len(keys) > 0 {
		err = AsNil(ps.HDel(context.TODO(), PrefixKey+ctx, keys...).Err())
		if err != nil {
			if strings.Contains(err.Error(), argNumErrorMsg) {
				log.Warn("This Redis version/implementation does not support variadic arguments for HDEL")
				for _, k := range keys {
					if err = AsNil(ps.HDel(context.TODO(), PrefixKey+ctx, k).Err()); err != nil {
						break
					}
				}
			}
		}
	}
	return
}

// Preservable - storage.DataStorage implementation
func (Connection) Preservable() bool {
	return true
}

// GC deletes all Peers from the PeerStorage which are older than the
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
func (ps *store) gc(cutoff time.Time) {
	log.Debug("storage: Redis: purging peers with no announces since", log.Fields{"before": cutoff})
	cutoffNanos := cutoff.UnixNano()
	// list all infoHashKeys in the group
	infoHashKeys, err := ps.SMembers(context.Background(), IHKey).Result()
	err = AsNil(err)
	if err == nil {
		for _, infoHashKey := range infoHashKeys {
			var cntKey string
			var seeder bool
			if seeder = strings.HasPrefix(infoHashKey, IH4SeederKey) || strings.HasPrefix(infoHashKey, IH6SeederKey); seeder {
				cntKey = CountSeederKey
			} else if strings.HasPrefix(infoHashKey, IH4LeecherKey) || strings.HasPrefix(infoHashKey, IH6LeecherKey) {
				cntKey = CountLeecherKey
			} else {
				log.Warn("storage: Redis: unexpected record found in info hash set", log.Fields{"infoHashKey": infoHashKey})
				continue
			}
			// list all (peer, timeout) pairs for the ih
			peerList, err := ps.HGetAll(context.Background(), infoHashKey).Result()
			err = AsNil(err)
			if err == nil {
				peersToRemove := make([]string, 0)
				for peerID, timeStamp := range peerList {
					if mtime, err := strconv.ParseInt(timeStamp, 10, 64); err == nil {
						if mtime <= cutoffNanos {
							log.Debug("storage: Redis: adding peer to remove list", log.Fields{"peerID": peerID})
							peersToRemove = append(peersToRemove, peerID)
						}
					} else {
						log.Error("storage: Redis: unable to decode peer timestamp", log.Fields{
							"infoHashKey": infoHashKey,
							"peerID":      peerID,
							"timestamp":   timeStamp,
							"error":       err,
						})
					}
				}
				if len(peersToRemove) > 0 {
					removedPeerCount, err := ps.HDel(context.Background(), infoHashKey, peersToRemove...).Result()
					err = AsNil(err)
					if err != nil {
						if strings.Contains(err.Error(), argNumErrorMsg) {
							log.Warn("This Redis version/implementation does not support variadic arguments for HDEL")
							for _, k := range peersToRemove {
								count, err := ps.HDel(context.Background(), infoHashKey, k).Result()
								err = AsNil(err)
								if err != nil {
									log.Error("storage: Redis: unable to delete peer", log.Fields{
										"infoHashKey": infoHashKey,
										"peerID":      k,
										"error":       err,
									})
								} else {
									removedPeerCount += count
								}
							}
						} else {
							log.Error("storage: Redis: unable to delete peers", log.Fields{
								"infoHashKey": infoHashKey,
								"peerIds":     peersToRemove,
								"error":       err,
							})
						}
					}
					if removedPeerCount > 0 { // DECR seeder/leecher counter
						if err = ps.DecrBy(context.Background(), cntKey, removedPeerCount).Err(); err != nil {
							log.Error("storage: Redis: unable to decrement seeder/leecher peer count", log.Fields{
								"infoHashKey": infoHashKey,
								"countKey":    cntKey,
								"error":       err,
							})
						}
					}
				}

				err = AsNil(ps.Watch(context.Background(), func(tx *redis.Tx) (err error) {
					var infoHashCount uint64
					infoHashCount, err = ps.HLen(context.Background(), infoHashKey).Uint64()
					err = AsNil(err)
					if err == nil && infoHashCount == 0 {
						// Empty hashes are not shown among existing keys,
						// in other words, it's removed automatically after `HDEL` the last field.
						// _, err := ps.Del(context.TODO(), infoHashKey)
						err = AsNil(ps.SRem(context.Background(), IHKey, infoHashKey).Err())
					}
					return err
				}, infoHashKey))
				if err != nil {
					log.Error("storage: Redis: unable to clean info hash records", log.Fields{
						"infoHashKey": infoHashKey,
						"error":       err,
					})
				}
			} else {
				log.Error("storage: Redis: unable to fetch info hash peers", log.Fields{
					"infoHashKey": infoHashKey,
					"error":       err,
				})
			}
		}
	} else {
		log.Error("storage: Redis: unable to fetch info hash set", log.Fields{"hashSet": IHKey, "error": err})
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
		if ps.UniversalClient != nil {
			log.Info("storage: Redis: exiting. mochi does not clear data in redis when exiting. mochi keys have prefix " + PrefixKey)
			err = ps.UniversalClient.Close()
			ps.UniversalClient = nil
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
