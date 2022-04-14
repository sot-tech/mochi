package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
)

var (
	driversM sync.RWMutex
	drivers  = make(map[string]Driver)
)

// Entry - some key-value pair, used for BulkPut
type Entry struct {
	Key   string
	Value any
}

// Driver is the interface used to initialize a new type of Storage.
type Driver interface {
	NewStorage(cfg any) (Storage, error)
}

// ErrResourceDoesNotExist is the error returned by all delete methods and the
// AnnouncePeers method of the Storage interface if the requested resource
// does not exist.
var ErrResourceDoesNotExist = bittorrent.ClientError("resource does not exist")

// ErrDriverDoesNotExist is the error returned by NewStorage when a peer
// store driver with that name does not exist.
var ErrDriverDoesNotExist = errors.New("peer store driver with that name does not exist")

// Storage is an interface that abstracts the interactions of storing and
// manipulating Peers such that it can be implemented for various data stores.
//
// Implementations of the Storage interface must do the following in addition
// to implementing the methods of the interface in the way documented:
//
// - Implement a garbage-collection strategy that ensures stale data is removed.
//     For example, a timestamp on each InfoHash/Peer combination can be used
//     to track the last activity for that Peer. The entire database can then
//     be scanned periodically and too old Peers removed. The intervals and
//     durations involved should be configurable.
// - IPv4 and IPv6 swarms must be isolated from each other.
//     A Storage must be able to transparently handle IPv4 and IPv6 Peers, but
//     must separate them. AnnouncePeers and ScrapeSwarm must return information
//     about the Swarm matching the given AddressFamily only.
//
// Implementations can be tested against this interface using the tests in
// storage_test.go and the benchmarks in storage_bench.go.
type Storage interface {
	// PutSeeder adds a Seeder to the Swarm identified by the provided
	// InfoHash.
	PutSeeder(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error

	// DeleteSeeder removes a Seeder from the Swarm identified by the
	// provided InfoHash.
	//
	// If the Swarm or Peer does not exist, this function returns
	// ErrResourceDoesNotExist.
	DeleteSeeder(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error

	// PutLeecher adds a Leecher to the Swarm identified by the provided
	// InfoHash.
	// If the Swarm does not exist already, it is created.
	PutLeecher(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error

	// DeleteLeecher removes a Leecher from the Swarm identified by the
	// provided InfoHash.
	//
	// If the Swarm or Peer does not exist, this function returns
	// ErrResourceDoesNotExist.
	DeleteLeecher(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error

	// GraduateLeecher promotes a Leecher to a Seeder in the Swarm
	// identified by the provided InfoHash.
	//
	// If the given Peer is not present as a Leecher or the swarm does not exist
	// already, the Peer is added as a Seeder and no error is returned.
	GraduateLeecher(infoHash bittorrent.InfoHash, peer bittorrent.Peer) error

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
	AnnouncePeers(infoHash bittorrent.InfoHash, seeder bool, numWant int, peer bittorrent.Peer) (peers []bittorrent.Peer, err error)

	// ScrapeSwarm returns information required to answer a Scrape request
	// about a Swarm identified by the given InfoHash.
	// The AddressFamily indicates whether or not the IPv6 swarm should be
	// scraped.
	// The Complete and Incomplete fields of the Scrape must be filled,
	// filling the Snatches field is optional.
	//
	// If the Swarm does not exist, an empty Scrape and no error is returned.
	ScrapeSwarm(infoHash bittorrent.InfoHash, peer bittorrent.Peer) bittorrent.Scrape

	// Put used to place arbitrary k-v data with specified context
	// into storage. ctx parameter used to group data
	// (i.e. data only for specific middleware module)
	Put(ctx string, value Entry) error

	// BulkPut used to place array of k-v data in specified context.
	// Useful when several data entries should be added in single transaction/connection
	BulkPut(ctx string, values ...Entry) error

	// Contains checks if any data in specified context exist
	Contains(ctx string, key string) (bool, error)

	// Load used to get arbitrary data in specified context by its key
	Load(ctx string, key string) (any, error)

	// Delete used to delete arbitrary data in specified context by its keys
	Delete(ctx string, keys ...string) error

	// GC used to delete stale data, such as timed out seeders/leechers
	GC(cutoff time.Time)

	// Stopper is an interface that expects a Stop method to stop the Storage.
	// For more details see the documentation in the stop package.
	stop.Stopper

	// Fielder returns a loggable version of the data used to configure and
	// operate a particular Storage.
	log.Fielder
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

	driversM.Lock()
	defer driversM.Unlock()

	if _, dup := drivers[name]; dup {
		panic("storage: RegisterDriver called twice for " + name)
	}

	drivers[name] = d
}

// NewStorage attempts to initialize a new Storage instance from
// the list of registered Drivers.
//
// If a driver does not exist, returns ErrDriverDoesNotExist.
func NewStorage(name string, cfg any) (ps Storage, err error) {
	driversM.RLock()
	defer driversM.RUnlock()

	var d Driver
	d, ok := drivers[name]
	if !ok {
		return nil, ErrDriverDoesNotExist
	}

	return d.NewStorage(cfg)
}
