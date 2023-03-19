// Package storage contains interfaces and register of storage provider and
// interface which should satisfy tracker storage
package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
)

const (
	// DefaultPrometheusReportingInterval default interval of statistics collection
	DefaultPrometheusReportingInterval = time.Second * 1
	// DefaultGarbageCollectionInterval default interval of stale peers deletions
	DefaultGarbageCollectionInterval = time.Minute * 3
	// DefaultPeerLifetime default peer lifetime
	DefaultPeerLifetime = time.Minute * 30
)

var (
	logger    = log.NewLogger("storage")
	driversMU sync.RWMutex
	drivers   = make(map[string]Driver)
)

// Config holds configuration for periodic execution tasks, which may or may not implement
// specific storage (such as GCAware or StatisticsAware)
type Config struct {
	// GarbageCollectionInterval period of GC
	GarbageCollectionInterval time.Duration `cfg:"gc_interval"`
	// PeerLifetime maximum TTL of peer
	PeerLifetime time.Duration `cfg:"peer_lifetime"`
	// PrometheusReportingInterval period of statistics data polling
	PrometheusReportingInterval time.Duration `cfg:"prometheus_reporting_interval"`
}

func (c Config) sanitizeGCConfig() (gcInterval, peerTTL time.Duration) {
	if c.GarbageCollectionInterval <= 0 {
		gcInterval = DefaultGarbageCollectionInterval
		logger.Warn().
			Str("name", "GarbageCollectionInterval").
			Dur("provided", c.GarbageCollectionInterval).
			Dur("default", DefaultGarbageCollectionInterval).
			Msg("falling back to default configuration")
	} else {
		gcInterval = c.GarbageCollectionInterval
	}
	if c.PeerLifetime <= 0 {
		peerTTL = DefaultPeerLifetime
		logger.Warn().
			Str("name", "PeerLifetime").
			Dur("provided", c.PeerLifetime).
			Dur("default", DefaultPeerLifetime).
			Msg("falling back to default configuration")
	} else {
		peerTTL = c.PeerLifetime
	}
	return
}

func (c Config) sanitizeStatisticsConfig() (statInterval time.Duration) {
	if c.PrometheusReportingInterval < 0 {
		statInterval = DefaultPrometheusReportingInterval
		logger.Warn().
			Str("name", "PrometheusReportingInterval").
			Dur("provided", c.PrometheusReportingInterval).
			Dur("default", DefaultPrometheusReportingInterval).
			Msg("falling back to default configuration")
	}
	return
}

// Entry - some key-value pair, used for BulkPut
type Entry struct {
	Key   string
	Value []byte
}

// Driver is the function used to initialize a new PeerStorage
// with provided configuration.
type Driver func(conf.MapConfig) (PeerStorage, error)

// ErrResourceDoesNotExist is the error returned by all delete methods and the
// AnnouncePeers method of the PeerStorage interface if the requested resource
// does not exist.
var ErrResourceDoesNotExist = bittorrent.ClientError("resource does not exist")

// DataStorage is the interface, used for implementing store for arbitrary data
type DataStorage interface {
	io.Closer
	// Put used to place arbitrary k-v data with specified context
	// into storage. storeCtx parameter used to group data
	// (i.e. data only for specific middleware module: hash key, table name etc...)
	Put(ctx context.Context, storeCtx string, values ...Entry) error

	// Contains checks if any data in specified context exist
	Contains(ctx context.Context, storeCtx string, key string) (bool, error)

	// Load used to get arbitrary data in specified context by its key
	Load(ctx context.Context, storeCtx string, key string) ([]byte, error)

	// Delete used to delete arbitrary data in specified context by its keys
	Delete(ctx context.Context, storeCtx string, keys ...string) error

	// Preservable indicates, that this storage can store data permanently,
	// in other words, is NOT in-memory storage, which data will be lost after restart
	Preservable() bool
}

// PeerStorage is an interface that abstracts the interactions of storing and
// manipulating Peers such that it can be implemented for various data stores.
//
// Implementations of the PeerStorage interface must do the following in addition
// to implementing the methods of the interface in the way documented:
//
//   - Implement a garbage-collection strategy that ensures stale data is removed.
//     For example, a timestamp on each InfoHash/Peer combination can be used
//     to track the last activity for that Peer. The entire database can then
//     be scanned periodically and too old Peers removed. The intervals and
//     durations involved should be configurable.
//   - IPv4 and IPv6 swarms may be isolated from each other.
//
// Implementations can be tested against this interface using the tests in
// storage_test.go and the benchmarks in storage_bench.go.
type PeerStorage interface {
	DataStorage
	// PutSeeder adds a Seeder to the Swarm identified by the provided
	// InfoHash.
	PutSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// DeleteSeeder removes a Seeder from the Swarm identified by the
	// provided InfoHash.
	//
	// If the Swarm or Peer does not exist, this function returns
	// ErrResourceDoesNotExist.
	DeleteSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// PutLeecher adds a Leecher to the Swarm identified by the provided
	// InfoHash.
	// If the Swarm does not exist already, it is created.
	PutLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// DeleteLeecher removes a Leecher from the Swarm identified by the
	// provided InfoHash.
	//
	// If the Swarm or Peer does not exist, this function returns
	// ErrResourceDoesNotExist.
	DeleteLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// GraduateLeecher promotes a Leecher to a Seeder in the Swarm
	// identified by the provided InfoHash.
	//
	// If the given Peer is not present as a Leecher or the swarm does not exist
	// already, the Peer is added as a Seeder and no error is returned.
	GraduateLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// AnnouncePeers is a best effort attempt to return Peers from the Swarm
	// identified by the provided InfoHash.
	// The numWant parameter indicates the number of peers requested by the
	// announcing Peer p. The seeder flag determines whether the Peer announced
	// as a Seeder.
	// The returned Peers are required to be either all IPv4 or all IPv6.
	//
	// The returned Peers should strive be:
	// - as close to length equal to numWant as possible without going over
	// - all IPv4 or all IPv6 depending on the provided peer
	// - if seeder is true, should ideally return more leechers than seeders
	// - if seeder is false, should ideally return more seeders than
	//   leechers
	//
	// Returns ErrResourceDoesNotExist if the provided InfoHash is not tracked.
	AnnouncePeers(ctx context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error)

	// ScrapeSwarm returns information required to answer a Scrape request
	// about a Swarm identified by the given InfoHash.
	// The AddressFamily indicates whether or not the IPv6 swarm should be
	// scraped.
	// The Complete and Incomplete fields of the Scrape must be filled,
	// filling the Snatches field is optional.
	//
	// If the Swarm does not exist, an empty Scrape and no error is returned.
	ScrapeSwarm(ctx context.Context, ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32, err error)

	// Ping used for checks if storage is alive
	// (connection could be established, enough space etc.)
	Ping(ctx context.Context) error
}

// GarbageCollector marks that this storage supports periodic
// stale peers collection
type GarbageCollector interface {
	// ScheduleGC used to delete stale data, such as timed out seeders/leechers.
	// Note: implementation must create subroutine by itself
	ScheduleGC(gcInterval, peerLifeTime time.Duration)
}

// StatisticsCollector marks that this storage supports periodic
// statistics collection
type StatisticsCollector interface {
	// ScheduleStatisticsCollection used to receive statistics information about hashes,
	// seeders and leechers count.
	// Note: implementation must create subroutine by itself
	ScheduleStatisticsCollection(reportInterval time.Duration)
}

// RegisterDriver makes a Driver available by the provided name.
//
// If called twice with the same name, the name is blank, or if the provided
// Driver is nil, this function panics.
func RegisterDriver(name string, d Driver) {
	if name == "" {
		panic("storage: could not register a Driver with an empty name")
	}
	if d == nil {
		panic("storage: could not register a nil Driver")
	}

	driversMU.Lock()
	defer driversMU.Unlock()

	if _, dup := drivers[name]; dup {
		panic("storage: RegisterDriver called twice for " + name)
	}

	drivers[name] = d
}

// NewStorage attempts to initialize a new PeerStorage instance from
// the list of registered drivers.
func NewStorage(cfg conf.NamedMapConfig) (ps PeerStorage, err error) {
	driversMU.RLock()
	defer driversMU.RUnlock()
	logger.Debug().Object("cfg", cfg).Msg("staring storage")

	var b Driver
	b, ok := drivers[cfg.Name]
	if !ok {
		return nil, fmt.Errorf("storage with name '%s' does not exists", cfg.Name)
	}

	c := new(Config)
	if err = cfg.Config.Unmarshal(c); err != nil {
		return
	}

	if ps, err = b(cfg.Config); err != nil {
		return
	}

	if gc, isOk := ps.(GarbageCollector); isOk {
		gcInterval, peerTTL := c.sanitizeGCConfig()
		logger.Info().
			Str("name", cfg.Name).
			Dur("gcInterval", gcInterval).
			Dur("peerTTL", peerTTL).
			Msg("scheduling GC")
		gc.ScheduleGC(gcInterval, peerTTL)
	} else {
		logger.Debug().
			Str("name", cfg.Name).
			Msg("storage does not support GC")
	}

	if st, isOk := ps.(StatisticsCollector); isOk {
		if statInterval := c.sanitizeStatisticsConfig(); statInterval > 0 {
			logger.Info().
				Str("name", cfg.Name).
				Dur("statInterval", statInterval).
				Msg("scheduling statistics collection")
			st.ScheduleStatisticsCollection(statInterval)
		} else {
			logger.Info().Str("name", cfg.Name).Msg("statistics collection disabled because of zero reporting interval")
		}
	} else {
		logger.Debug().
			Str("name", cfg.Name).
			Msg("storage does not support statistics collection")
	}

	logger.Info().Str("name", cfg.Name).Msg("storage started")

	return
}
