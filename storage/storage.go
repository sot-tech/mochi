// Package storage contains interfaces and register of storage provider and
// interface which should satisfy tracker storage
package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
)

const (
	defaultPrometheusReportingInterval = time.Second * 1
	defaultGarbageCollectionInterval   = time.Minute * 3
	defaultPeerLifetime                = time.Minute * 30
)

var (
	logger   = log.NewLogger("storage configurator")
	driversM sync.RWMutex
	drivers  = make(map[string]Builder)
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
		gcInterval = defaultGarbageCollectionInterval
		logger.Warn().
			Str("name", "GarbageCollectionInterval").
			Dur("provided", c.GarbageCollectionInterval).
			Dur("default", defaultGarbageCollectionInterval).
			Msg("falling back to default configuration")
	} else {
		gcInterval = c.GarbageCollectionInterval
	}
	if c.PeerLifetime <= 0 {
		peerTTL = defaultPeerLifetime
		logger.Warn().
			Str("name", "PeerLifetime").
			Dur("provided", c.PeerLifetime).
			Dur("default", defaultPeerLifetime).
			Msg("falling back to default configuration")
	} else {
		peerTTL = c.PeerLifetime
	}
	return
}

func (c Config) sanitizeStatisticsConfig() (statInterval time.Duration) {
	if c.PrometheusReportingInterval < 0 {
		statInterval = defaultPrometheusReportingInterval
		logger.Warn().
			Str("name", "PrometheusReportingInterval").
			Dur("provided", c.PrometheusReportingInterval).
			Dur("default", defaultPrometheusReportingInterval).
			Msg("falling back to default configuration")
	}
	return
}

// Entry - some key-value pair, used for BulkPut
type Entry struct {
	Key   string
	Value any
}

// Builder is the function used to initialize a new type of PeerStorage.
type Builder func(cfg conf.MapConfig) (PeerStorage, error)

var (
	// ErrResourceDoesNotExist is the error returned by all delete methods and the
	// AnnouncePeers method of the PeerStorage interface if the requested resource
	// does not exist.
	ErrResourceDoesNotExist = bittorrent.ClientError("resource does not exist")

	// ErrDriverDoesNotExist is the error returned by NewStorage when a peer
	// store driver with that name does not exist.
	ErrDriverDoesNotExist = errors.New("peer store driver with that name does not exist")
)

// DataStorage is the interface, used for implementing store for arbitrary data
type DataStorage interface {
	// Put used to place arbitrary k-v data with specified context
	// into storage. ctx parameter used to group data
	// (i.e. data only for specific middleware module: hash key, table name etc...)
	Put(ctx string, values ...Entry) error

	// Contains checks if any data in specified context exist
	Contains(ctx string, key string) (bool, error)

	// Load used to get arbitrary data in specified context by its key
	Load(ctx string, key string) (any, error)

	// Delete used to delete arbitrary data in specified context by its keys
	Delete(ctx string, keys ...string) error

	// Preservable indicates, that this storage can store data permanently,
	// in other words, is NOT in-memory storage, which data will be lost after restart
	Preservable() bool
}

// GCAware is the interface for storage that supports periodic
// stale peers collection
type GCAware interface {
	// ScheduleGC used to delete stale data, such as timed out seeders/leechers.
	// Note: implementation must create subroutine by itself
	ScheduleGC(gcInterval, peerLifeTime time.Duration)
}

// StatisticsAware is the interface for storage that supports periodic
// statistics collection
type StatisticsAware interface {
	// ScheduleStatisticsCollection used to receive statistics information about hashes,
	// seeders and leechers count.
	// Note: implementation must create subroutine by itself
	ScheduleStatisticsCollection(reportInterval time.Duration)
}

// PeerStorage is an interface that abstracts the interactions of storing and
// manipulating Peers such that it can be implemented for various data stores.
//
// Implementations of the PeerStorage interface must do the following in addition
// to implementing the methods of the interface in the way documented:
//
// - Implement a garbage-collection strategy that ensures stale data is removed.
//     For example, a timestamp on each InfoHash/Peer combination can be used
//     to track the last activity for that Peer. The entire database can then
//     be scanned periodically and too old Peers removed. The intervals and
//     durations involved should be configurable.
// - IPv4 and IPv6 swarms may be isolated from each other.
//
// Implementations can be tested against this interface using the tests in
// storage_test.go and the benchmarks in storage_bench.go.
type PeerStorage interface {
	DataStorage
	// PutSeeder adds a Seeder to the Swarm identified by the provided
	// InfoHash.
	PutSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// DeleteSeeder removes a Seeder from the Swarm identified by the
	// provided InfoHash.
	//
	// If the Swarm or Peer does not exist, this function returns
	// ErrResourceDoesNotExist.
	DeleteSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// PutLeecher adds a Leecher to the Swarm identified by the provided
	// InfoHash.
	// If the Swarm does not exist already, it is created.
	PutLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// DeleteLeecher removes a Leecher from the Swarm identified by the
	// provided InfoHash.
	//
	// If the Swarm or Peer does not exist, this function returns
	// ErrResourceDoesNotExist.
	DeleteLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error

	// GraduateLeecher promotes a Leecher to a Seeder in the Swarm
	// identified by the provided InfoHash.
	//
	// If the given Peer is not present as a Leecher or the swarm does not exist
	// already, the Peer is added as a Seeder and no error is returned.
	GraduateLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error

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
	AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error)

	// ScrapeSwarm returns information required to answer a Scrape request
	// about a Swarm identified by the given InfoHash.
	// The AddressFamily indicates whether or not the IPv6 swarm should be
	// scraped.
	// The Complete and Incomplete fields of the Scrape must be filled,
	// filling the Snatches field is optional.
	//
	// If the Swarm does not exist, an empty Scrape and no error is returned.
	ScrapeSwarm(ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32)

	// Ping used for checks if storage is alive
	// (connection could be established, enough space etc.)
	Ping() error

	// Stopper is an interface that expects a Stop method to stop the PeerStorage.
	// For more details see the documentation in the stop package.
	stop.Stopper

	// LogObjectMarshaler returns a loggable version of the data used to configure and
	// operate a particular PeerStorage.
	zerolog.LogObjectMarshaler
}

// RegisterBuilder makes a Builder available by the provided name.
//
// If called twice with the same name, the name is blank, or if the provided
// Driver is nil, this function panics.
func RegisterBuilder(name string, b Builder) {
	if name == "" {
		panic("storage: could not register a Builder with an empty name")
	}
	if b == nil {
		panic("storage: could not register a nil Builder")
	}

	driversM.Lock()
	defer driversM.Unlock()

	if _, dup := drivers[name]; dup {
		panic("storage: RegisterBuilder called twice for " + name)
	}

	drivers[name] = b
}

// NewStorage attempts to initialize a new PeerStorage instance from
// the list of registered Drivers.
//
// If a builder does not exist, returns ErrDriverDoesNotExist.
func NewStorage(name string, cfg conf.MapConfig) (ps PeerStorage, err error) {
	driversM.RLock()
	defer driversM.RUnlock()

	var b Builder
	b, ok := drivers[name]
	if !ok {
		return nil, ErrDriverDoesNotExist
	}

	c := new(Config)
	if err = cfg.Unmarshal(c); err != nil {
		return
	}

	if ps, err = b(cfg); err != nil {
		return
	}

	if gc, isOk := ps.(GCAware); isOk {
		gcInterval, peerTTL := c.sanitizeGCConfig()
		logger.Info().
			Str("type", name).
			Dur("gcInterval", gcInterval).
			Dur("peerTTL", peerTTL).
			Msg("scheduling GC")
		gc.ScheduleGC(gcInterval, peerTTL)
	} else {
		logger.Debug().
			Str("type", name).
			Msg("storage does not support GC")
	}

	if st, isOk := ps.(StatisticsAware); isOk {
		if statInterval := c.sanitizeStatisticsConfig(); statInterval > 0 {
			logger.Info().
				Str("type", name).
				Dur("statInterval", statInterval).
				Msg("scheduling statistics collection")
			st.ScheduleStatisticsCollection(statInterval)
		} else {
			logger.Info().Str("type", name).Msg("statistics collection disabled because of zero reporting interval")
		}
	} else {
		logger.Debug().
			Str("type", name).
			Msg("storage does not support statistics collection")
	}

	return
}
