// Package redis implements the storage interface.
// BitTorrent tracker keeping peer data in redis with hash.
// There three categories of hash:
//
//   - CHI_{L,S}{4,6}_<HASH> (hash type)
//     To save peers that hold the infohash, used for fast searching,
//     deleting, and timeout handling
//
//   - CHI_I (set type)
//     To save all the infohashes, used for garbage collection,
//     metrics aggregation and leecher graduation
//
//   - CHI_D (hash type)
//     To record the number of torrent downloads.
//
// Two keys are used to record the count of seeders and leechers.
//
//   - CHI_C_S (key type)
//     To record the number of seeders.
//
//   - CHI_C_L (key type)
//     To record the number of leechers.
package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sot-tech/mochi/pkg/str2bytes"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

const (
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
	// CountDownloadsKey redis key for snatches (downloads) count
	CountDownloadsKey = "CHI_D"
)

var (
	logger = log.NewLogger("storage/redis")
	// errSentinelAndClusterChecked returned from initializer if both Config.Sentinel and Config.Cluster provided
	errSentinelAndClusterChecked = errors.New("unable to use both cluster and sentinel mode")
)

func init() {
	// Register the storage builder.
	storage.RegisterDriver("redis", builder{})
}

type builder struct{}

func (builder) NewPeerStorage(icfg conf.MapConfig) (storage.PeerStorage, error) {
	var cfg Config
	var err error

	if err = icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return NewStore(cfg)
}

func (b builder) NewDataStorage(icfg conf.MapConfig) (storage.DataStorage, error) {
	return b.NewPeerStorage(icfg)
}

// NewStore creates new redis peer storage with provided configuration structure
func NewStore(cfg Config) (storage.PeerStorage, error) {
	cfg, err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	rs, err := cfg.Connect()
	if err != nil {
		return nil, err
	}

	return &store{Connection: rs, closed: make(chan any)}, nil
}

// Config holds the configuration of a redis PeerStorage.
type Config struct {
	PeerLifetime   time.Duration `cfg:"peer_lifetime"`
	Addresses      []string
	TLS            bool
	CACerts        []string `cfg:"ca_certs"`
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

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() (Config, error) {
	if cfg.Sentinel && cfg.Cluster {
		return cfg, errSentinelAndClusterChecked
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
		logger.Warn().
			Str("name", "addresses").
			Strs("provided", cfg.Addresses).
			Strs("default", validCfg.Addresses).
			Msg("falling back to default configuration")
	}

	if cfg.ReadTimeout <= 0 {
		validCfg.ReadTimeout = defaultReadTimeout
		logger.Warn().
			Str("name", "readTimeout").
			Dur("provided", cfg.ReadTimeout).
			Dur("default", validCfg.ReadTimeout).
			Msg("falling back to default configuration")
	}

	if cfg.WriteTimeout <= 0 {
		validCfg.WriteTimeout = defaultWriteTimeout
		logger.Warn().
			Str("name", "writeTimeout").
			Dur("provided", cfg.WriteTimeout).
			Dur("default", validCfg.WriteTimeout).
			Msg("falling back to default configuration")
	}

	if cfg.ConnectTimeout <= 0 {
		validCfg.ConnectTimeout = defaultConnectTimeout
		logger.Warn().
			Str("name", "connectTimeout").
			Dur("provided", cfg.ConnectTimeout).
			Dur("default", validCfg.ConnectTimeout).
			Msg("falling back to default configuration")
	}

	if cfg.TLS {
		for _, cert := range cfg.CACerts {
			if _, err := os.Stat(cert); err != nil {
				return cfg, err
			}
		}
	}

	return validCfg, nil
}

// Connect creates redis client from configuration
func (cfg Config) Connect() (con Connection, err error) {
	var rs redis.UniversalClient
	var tlsConf *tls.Config
	if cfg.TLS {
		tlsConf = &tls.Config{MinVersion: tls.VersionTLS12}
		if len(cfg.CACerts) > 0 {
			certPool := x509.NewCertPool()
			var ok bool
			for _, cert := range cfg.CACerts {
				var certData []byte
				if certData, err = os.ReadFile(cert); err != nil {
					logger.Warn().
						Err(err).
						Str("path", cert).
						Msg("unable to read certificate(s) file")
				} else if added := certPool.AppendCertsFromPEM(certData); added {
					ok = true
				} else {
					logger.Warn().
						Str("path", cert).
						Msg("unable to append certificate(s) to trusted pool")
				}
			}
			if ok {
				tlsConf.RootCAs = certPool
			}
		}
	}
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
			TLSConfig:    tlsConf,
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
			TLSConfig:        tlsConf,
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
			TLSConfig:    tlsConf,
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
				duration := time.Since(start)
				logger.Debug().Dur("timeTaken", duration).Msg("gc complete")
				storage.PromGCDurationMilliseconds.Observe(float64(duration.Milliseconds()))
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
					logger.Debug().TimeDiff("timeTaken", time.Now(), before).Msg("populate prom complete")
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
	closed     chan any
	wg         sync.WaitGroup
	onceCloser sync.Once
}

func (ps *store) count(key string, getLength bool) (n uint64) {
	var err error
	if getLength {
		n, err = ps.SCard(context.Background(), key).Uint64()
	} else {
		n, err = ps.Get(context.Background(), key).Uint64()
	}
	err = NoResultErr(err)
	if err != nil {
		logger.Error().Err(err).Str("key", key).Msg("GET/SCARD failure")
	}
	return n
}

func (ps *store) getClock() int64 {
	return timecache.NowUnixNano()
}

func (ps *store) tx(ctx context.Context, txf func(tx redis.Pipeliner) error) (err error) {
	if pipe, txErr := ps.TxPipelined(ctx, txf); txErr == nil {
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
	return err
}

// NoResultErr returns nil if provided err is redis.Nil
// otherwise returns err
func NoResultErr(err error) error {
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
	return infoHashKey
}

func (ps *store) putPeer(ctx context.Context, infoHashKey, peerCountKey, peerID string) error {
	logger.Trace().
		Str("infoHashKey", infoHashKey).
		Str("peerID", peerID).
		Msg("put peer")
	return ps.tx(ctx, func(tx redis.Pipeliner) (err error) {
		if err = tx.HSet(ctx, infoHashKey, peerID, ps.getClock()).Err(); err != nil {
			return err
		}
		if err = tx.Incr(ctx, peerCountKey).Err(); err != nil {
			return err
		}
		err = tx.SAdd(ctx, IHKey, infoHashKey).Err()
		return err
	})
}

func (ps *store) delPeer(ctx context.Context, infoHashKey, peerCountKey, peerID string) error {
	logger.Trace().
		Str("infoHashKey", infoHashKey).
		Str("peerID", peerID).
		Msg("del peer")
	deleted, err := ps.HDel(ctx, infoHashKey, peerID).Uint64()
	err = NoResultErr(err)
	if err == nil {
		if deleted == 0 {
			err = storage.ErrResourceDoesNotExist
		} else {
			err = ps.Decr(ctx, peerCountKey).Err()
		}
	}

	return err
}

// PackPeer generates concatenation of PeerID, net port and IP-address
func PackPeer(p bittorrent.Peer) string {
	ip := p.Addr()
	b := make([]byte, bittorrent.PeerIDLen+2+(ip.BitLen()/8))
	copy(b[:bittorrent.PeerIDLen], p.ID.Bytes())
	binary.BigEndian.PutUint16(b[bittorrent.PeerIDLen:bittorrent.PeerIDLen+2], p.Port())
	copy(b[bittorrent.PeerIDLen+2:], ip.AsSlice())
	return str2bytes.BytesToString(b)
}

func (ps *store) PutSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return ps.putPeer(ctx, InfoHashKey(ih.RawString(), true, peer.Addr().Is6()), CountSeederKey, PackPeer(peer))
}

func (ps *store) DeleteSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return ps.delPeer(ctx, InfoHashKey(ih.RawString(), true, peer.Addr().Is6()), CountSeederKey, PackPeer(peer))
}

func (ps *store) PutLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return ps.putPeer(ctx, InfoHashKey(ih.RawString(), false, peer.Addr().Is6()), CountLeecherKey, PackPeer(peer))
}

func (ps *store) DeleteLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return ps.delPeer(ctx, InfoHashKey(ih.RawString(), false, peer.Addr().Is6()), CountLeecherKey, PackPeer(peer))
}

func (ps *store) GraduateLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Msg("graduate leecher")

	infoHash, peerID, isV6 := ih.RawString(), PackPeer(peer), peer.Addr().Is6()
	ihSeederKey, ihLeecherKey := InfoHashKey(infoHash, true, isV6), InfoHashKey(infoHash, false, isV6)

	return ps.tx(ctx, func(tx redis.Pipeliner) error {
		deleted, err := tx.HDel(ctx, ihLeecherKey, peerID).Uint64()
		err = NoResultErr(err)
		if err == nil {
			if deleted > 0 {
				err = tx.Decr(ctx, CountLeecherKey).Err()
			}
		}
		if err == nil {
			err = tx.HSet(ctx, ihSeederKey, peerID, ps.getClock()).Err()
		}
		if err == nil {
			err = tx.Incr(ctx, CountSeederKey).Err()
		}
		if err == nil {
			err = tx.SAdd(ctx, IHKey, ihSeederKey).Err()
		}
		if err == nil {
			err = tx.HIncrBy(ctx, CountDownloadsKey, infoHash, 1).Err()
		}
		return err
	})
}

// peerMinimumLen is the least allowed length of string serialized Peer
const peerMinimumLen = bittorrent.PeerIDLen + 2 + net.IPv4len

var errInvalidPeerDataSize = fmt.Errorf("invalid peer data (must be at least %d bytes (PeerID + Port + IPv4))",
	peerMinimumLen)

// UnpackPeer constructs Peer from serialized by Peer.PackPeer data: PeerID[20by]Port[2by]net.IP[4/16by]
func UnpackPeer(data string) (peer bittorrent.Peer, err error) {
	if len(data) < peerMinimumLen {
		err = errInvalidPeerDataSize
		return peer, err
	}
	b := str2bytes.StringToBytes(data)
	peerID, _ := bittorrent.NewPeerID(b[:bittorrent.PeerIDLen])
	if addr, isOk := netip.AddrFromSlice(b[bittorrent.PeerIDLen+2:]); isOk {
		peer = bittorrent.Peer{
			ID: peerID,
			AddrPort: netip.AddrPortFrom(
				addr.Unmap(),
				binary.BigEndian.Uint16(b[bittorrent.PeerIDLen:bittorrent.PeerIDLen+2]),
			),
		}
	} else {
		err = bittorrent.ErrInvalidIP
	}

	return peer, err
}

func (ps *Connection) parsePeersList(peersResult *redis.StringSliceCmd) (peers []bittorrent.Peer, err error) {
	var peerIDs []string
	peerIDs, err = peersResult.Result()
	if err = NoResultErr(err); err == nil {
		for _, peerID := range peerIDs {
			if p, err := UnpackPeer(peerID); err == nil {
				peers = append(peers, p)
			} else {
				logger.Error().Err(err).Str("peerID", peerID).Msg("unable to decode peer")
			}
		}
	}
	return peers, err
}

type getPeersFn func(context.Context, string, int) *redis.StringSliceCmd

// GetPeers retrieves peers for provided info hash by calling membersFn and
// converts result to bittorrent.Peer array.
// If forSeeder set to true - returns only leechers, if false -
// seeders and if maxCount not reached - leechers.
func (ps *Connection) GetPeers(
	ctx context.Context, ih bittorrent.InfoHash, forSeeder bool, maxCount int, isV6 bool, membersFn getPeersFn,
) (out []bittorrent.Peer, err error) {
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
		peers, err = ps.parsePeersList(membersFn(ctx, infoHashKey, maxCount))
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
		logger.Warn().Err(err).Stringer("infoHash", ih).Msg("error occurred while retrieving peers")
	}

	return out, err
}

func (ps *store) AnnouncePeers(
	ctx context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool,
) ([]bittorrent.Peer, error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Bool("forSeeder", forSeeder).
		Int("numWant", numWant).
		Bool("v6", v6).
		Msg("announce peers")

	return ps.GetPeers(ctx, ih, forSeeder, numWant, v6, ps.HRandField)
}

type getPeerCountFn func(context.Context, string) *redis.IntCmd

// ScrapeIH calls provided countFn and returns seeders, leechers and downloads count for specified info hash
func (ps *Connection) ScrapeIH(ctx context.Context, ih bittorrent.InfoHash, countFn getPeerCountFn) (
	leechersCount, seedersCount, downloadsCount uint32, err error,
) {
	infoHash := ih.RawString()
	var lc4, lc6, sc4, sc6, dc int64

	lc4, err = countFn(ctx, InfoHashKey(infoHash, false, false)).Result()
	if err = NoResultErr(err); err != nil {
		return leechersCount, seedersCount, downloadsCount, err
	}
	lc6, err = countFn(ctx, InfoHashKey(infoHash, false, true)).Result()
	if err = NoResultErr(err); err != nil {
		return leechersCount, seedersCount, downloadsCount, err
	}
	sc4, err = countFn(ctx, InfoHashKey(infoHash, true, false)).Result()
	if err = NoResultErr(err); err != nil {
		return leechersCount, seedersCount, downloadsCount, err
	}
	sc6, err = countFn(ctx, InfoHashKey(infoHash, true, true)).Result()
	if err = NoResultErr(err); err != nil {
		return leechersCount, seedersCount, downloadsCount, err
	}
	dc, err = ps.HGet(ctx, CountDownloadsKey, infoHash).Int64()
	if err = NoResultErr(err); err != nil {
		return leechersCount, seedersCount, downloadsCount, err
	}
	leechersCount, seedersCount, downloadsCount = uint32(lc4+lc6), uint32(sc4+sc6), uint32(dc)
	return leechersCount, seedersCount, downloadsCount, err
}

func (ps *store) ScrapeSwarm(ctx context.Context, ih bittorrent.InfoHash) (uint32, uint32, uint32, error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Msg("scrape swarm")
	return ps.ScrapeIH(ctx, ih, ps.HLen)
}

const argNumErrorMsg = "ERR wrong number of arguments"

// Put - storage.DataStorage implementation
func (ps *Connection) Put(ctx context.Context, storeCtx string, values ...storage.Entry) (err error) {
	if l := len(values); l > 0 {
		if l == 1 {
			err = ps.HSet(ctx, PrefixKey+storeCtx, values[0].Key, values[0].Value).Err()
		} else {
			args := make([]any, 0, l*2)
			for _, p := range values {
				args = append(args, p.Key, p.Value)
			}
			err = ps.HSet(ctx, PrefixKey+storeCtx, args...).Err()
			if err != nil {
				if strings.Contains(err.Error(), argNumErrorMsg) {
					logger.Warn().Msg("This Redis version/implementation does not support variadic arguments for HSET")
					for _, p := range values {
						if err = ps.HSet(ctx, PrefixKey+storeCtx, p.Key, p.Value).Err(); err != nil {
							break
						}
					}
				}
			}
		}
	}
	return err
}

// Contains - storage.DataStorage implementation
func (ps *Connection) Contains(ctx context.Context, storeCtx string, key string) (bool, error) {
	exist, err := ps.HExists(ctx, PrefixKey+storeCtx, key).Result()
	return exist, NoResultErr(err)
}

// Load - storage.DataStorage implementation
func (ps *Connection) Load(ctx context.Context, storeCtx string, key string) (v []byte, err error) {
	v, err = ps.HGet(ctx, PrefixKey+storeCtx, key).Bytes()
	if err != nil && errors.Is(err, redis.Nil) {
		v, err = nil, nil
	}
	return v, err
}

// Delete - storage.DataStorage implementation
func (ps *Connection) Delete(ctx context.Context, storeCtx string, keys ...string) (err error) {
	if len(keys) > 0 {
		err = NoResultErr(ps.HDel(ctx, PrefixKey+storeCtx, keys...).Err())
		if err != nil {
			if strings.Contains(err.Error(), argNumErrorMsg) {
				logger.Warn().Msg("This Redis version/implementation does not support variadic arguments for HDEL")
				for _, k := range keys {
					if err = NoResultErr(ps.HDel(ctx, PrefixKey+storeCtx, k).Err()); err != nil {
						break
					}
				}
			}
		}
	}
	return err
}

// Preservable - storage.DataStorage implementation
func (*Connection) Preservable() bool {
	return true
}

// Ping sends `PING` request to Redis server
func (ps *Connection) Ping(ctx context.Context) error {
	return ps.UniversalClient.Ping(ctx).Err()
}

// GC deletes all Peers from the PeerStorage which are older than the
// cutoff time.
//
// This function must be able to execute while other methods on this interface
// are being executed in parallel.
//
//   - The Delete(Seeder|Leecher) and GraduateLeecher methods never delete an
//     infohash key from an addressFamily hash. They also never decrement the
//     infohash counter.
//   - The Put(Seeder|Leecher) and GraduateLeecher methods only ever add infohash
//     keys to addressFamily hashes and increment the infohash counter.
//   - The only method that deletes from the addressFamily hashes is
//     gc, which also decrements the counters. That means that,
//     even if a Delete(Seeder|Leecher) call removes the last peer from a swarm,
//     the infohash counter is not changed and the infohash is left in the
//     addressFamily hash until it will be cleaned up by gc.
//   - gc must run regularly.
//   - A WATCH ... MULTI ... EXEC block fails, if between the WATCH and the 'EXEC'
//     any of the watched keys have changed. The location of the 'MULTI' doesn't
//     matter.
//
// We have to analyze four cases to prove our algorithm works. I'll characterize
// them by a tuple (number of peers in a swarm before WATCH, number of peers in
// the swarm during the transaction).
//
//  1. (0,0), the easy case: The swarm is empty, we watch the key, we execute
//     HLEN and find it empty. We remove it and decrement the counter. It stays
//     empty the entire time, the transaction goes through.
//  2. (1,n > 0): The swarm is not empty, we watch the key, we find it non-empty,
//     we unwatch the key. All good. No transaction is made, no transaction fails.
//  3. (0,1): We have to analyze this in two ways.
//     - If the change happens before the HLEN call, we will see that the swarm is
//     not empty and start no transaction.
//     - If the change happens after the HLEN, we will attempt a transaction and it
//     will fail. This is okay, the swarm is not empty, we will try cleaning it up
//     next time gc runs.
//  4. (1,0): Again, two ways:
//     - If the change happens before the HLEN, we will see an empty swarm. This
//     situation happens if a call to Delete(Seeder|Leecher) removed the last
//     peer asynchronously. We will attempt a transaction, but the transaction
//     will fail. This is okay, the infohash key will remain in the addressFamily
//     hash, we will attempt to clean it up the next time 'gc` runs.
//     - If the change happens after the HLEN, we will not even attempt to make the
//     transaction. The infohash key will remain in the addressFamil hash and
//     we'll attempt to clean it up the next time gc runs.
func (ps *store) gc(cutoff time.Time) {
	cutoffNanos := cutoff.UnixNano()
	// list all infoHashKeys in the group
	infoHashKeys, err := ps.SMembers(context.Background(), IHKey).Result()
	err = NoResultErr(err)
	if err == nil {
		for _, infoHashKey := range infoHashKeys {
			var cntKey string
			var seeder bool
			if seeder = strings.HasPrefix(infoHashKey, IH4SeederKey) || strings.HasPrefix(infoHashKey,
				IH6SeederKey); seeder {
				cntKey = CountSeederKey
			} else if strings.HasPrefix(infoHashKey, IH4LeecherKey) || strings.HasPrefix(infoHashKey, IH6LeecherKey) {
				cntKey = CountLeecherKey
			} else {
				logger.Warn().Str("infoHashKey", infoHashKey).Msg("unexpected record found in info hash set")
				continue
			}
			// list all (peer, timeout) pairs for the ih
			peerList, err := ps.HGetAll(context.Background(), infoHashKey).Result()
			err = NoResultErr(err)
			if err == nil {
				peersToRemove := make([]string, 0)
				for peerID, timeStamp := range peerList {
					if mtime, err := strconv.ParseInt(timeStamp, 10, 64); err == nil {
						if mtime <= cutoffNanos {
							logger.Trace().Str("peerID", peerID).Msg("adding peer to remove list")
							peersToRemove = append(peersToRemove, peerID)
						}
					} else {
						logger.Error().Err(err).
							Str("infoHashKey", infoHashKey).
							Str("peerID", peerID).
							Str("timestamp", timeStamp).
							Msg("unable to decode peer timestamp")
					}
				}
				if len(peersToRemove) > 0 {
					removedPeerCount, err := ps.HDel(context.Background(), infoHashKey, peersToRemove...).Result()
					err = NoResultErr(err)
					if err != nil {
						if strings.Contains(err.Error(), argNumErrorMsg) {
							logger.Warn().Msg("This Redis version/implementation does not support variadic arguments for HDEL")
							for _, k := range peersToRemove {
								count, err := ps.HDel(context.Background(), infoHashKey, k).Result()
								err = NoResultErr(err)
								if err != nil {
									logger.Error().Err(err).
										Str("infoHashKey", infoHashKey).
										Str("peerID", k).
										Msg("unable to delete peer")
								} else {
									removedPeerCount += count
								}
							}
						} else {
							logger.Error().Err(err).
								Str("infoHashKey", infoHashKey).
								Strs("peerIDs", peersToRemove).
								Msg("unable to delete peers")
						}
					}
					if removedPeerCount > 0 { // DECR seeder/leecher counter
						if err = ps.DecrBy(context.Background(), cntKey, removedPeerCount).Err(); err != nil {
							logger.Error().Err(err).
								Str("infoHashKey", infoHashKey).
								Str("countKey", cntKey).
								Msg("unable to decrement seeder/leecher peer count")
						}
					}
				}

				err = NoResultErr(ps.Watch(context.Background(), func(_ *redis.Tx) (err error) {
					var infoHashCount uint64
					infoHashCount, err = ps.HLen(context.Background(), infoHashKey).Uint64()
					err = NoResultErr(err)
					if err == nil && infoHashCount == 0 {
						// Empty hashes are not shown among existing keys,
						// in other words, it's removed automatically after `HDEL` the last field.
						err = NoResultErr(ps.SRem(context.Background(), IHKey, infoHashKey).Err())
					}
					return err
				}, infoHashKey))
				if err != nil {
					logger.Error().Err(err).
						Str("infoHashKey", infoHashKey).
						Msg("unable to clean info hash records")
				}
			} else {
				logger.Error().Err(err).
					Str("infoHashKey", infoHashKey).
					Msg("unable to fetch info hash peers")
			}
		}
	} else {
		logger.Error().Err(err).
			Str("hashSet", IHKey).
			Msg("unable to fetch info hash peers")
	}
}

func (ps *store) Close() (err error) {
	ps.onceCloser.Do(func() {
		close(ps.closed)
		ps.wg.Wait()
		logger.Info().Msg("redis exiting. mochi does not clear data in redis when exiting. mochi keys have prefix " + PrefixKey)
		err = ps.UniversalClient.Close()
	})
	return err
}
