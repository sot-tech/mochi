package bittorrent

import (
	"net/netip"
	"sort"
	"time"

	"github.com/rs/zerolog"
)

// RequestAddress wrapper for netip.Addr with Provided flag.
// Used in RequestAddresses to determine addresses priority
type RequestAddress struct {
	netip.Addr
	Provided bool
}

// Note: there is no IPv6 broadcast address
var globalBroadcastIPv4 = netip.AddrFrom4([4]byte{255, 255, 255, 255})

// IsValid checks if netip.Addr is valid, not unspecified and not multicast
func (a RequestAddress) IsValid() bool {
	return a.Addr.IsValid() && !(a.IsUnspecified() || a.IsMulticast() || a.Addr == globalBroadcastIPv4)
}

// MarshalZerologObject writes fields into zerolog event
func (a RequestAddress) MarshalZerologObject(e *zerolog.Event) {
	e.Stringer("address", a.Addr).Bool("provided", a.Provided)
}

// RequestAddresses is an array of RequestAddress used mainly for
// sort.Interface implementation.
// Frontends may determine peer's address from connections info
// or from provided values or combine these addresses to fetch maximum
// connection information about peer
type RequestAddresses []RequestAddress

func (aa RequestAddresses) Len() int {
	return len(aa)
}

// Less returns true only if i-th RequestAddress is marked as
// RequestAddress.Provided and j-th is not (provided address has
// higher priority)
func (aa RequestAddresses) Less(i, j int) bool {
	return aa[i].Provided && !aa[j].Provided
}

func (aa RequestAddresses) Swap(i, j int) {
	aa[i], aa[j] = aa[j], aa[i]
}

// Add checks if provided RequestAddress is valid and adds unmapped
// netip.Addr to array
func (aa *RequestAddresses) Add(a RequestAddress) {
	if a.IsValid() {
		a.Addr = a.Unmap()
		*aa = append(*aa, a)
	}
}

// Sanitize checks if array is not empty and at least one RequestAddress is valid,
// then make them unique and sorts with RequestAddresses.Less rule.
// If ignorePrivate set to true, function will preserve only global unicast and
// non-private (see netip.IsGlobalUnicast and netip.IsPrivate) addresses.
// If there are no valid and global (if ignorePrivate checked) addresses in array,
// function returns false and empty receiver.
func (aa *RequestAddresses) Sanitize(ignorePrivate bool) bool {
	if len(*aa) == 0 {
		return false
	}
	uniqueAddresses := make(map[netip.Addr]bool, len(*aa))
	for _, a := range *aa {
		if a.IsValid() && (!ignorePrivate || a.IsGlobalUnicast() && !a.IsPrivate()) {
			if provided, found := uniqueAddresses[a.Addr]; !found || !provided && a.Provided {
				uniqueAddresses[a.Addr] = a.Provided
			}
		}
	}
	*aa = make(RequestAddresses, 0, len(uniqueAddresses))
	for a, p := range uniqueAddresses {
		*aa = append(*aa, RequestAddress{a, p})
	}
	if len(*aa) > 1 {
		sort.Sort(*aa)
	}
	return len(uniqueAddresses) > 0
}

// GetFirst returns first address from array
// or empty netip.Addr if array is empty
func (aa RequestAddresses) GetFirst() netip.Addr {
	var a netip.Addr
	if len(aa) > 0 {
		a = aa[0].Addr
	}
	return a
}

// MarshalZerologArray writes array elements to zerolog event
func (aa RequestAddresses) MarshalZerologArray(a *zerolog.Array) {
	for _, addr := range aa {
		a.Object(addr)
	}
}

// Peers wrapper of array of Peer-s
type Peers []Peer

// MarshalZerologArray writes array elements to zerolog event
func (p Peers) MarshalZerologArray(a *zerolog.Array) {
	for _, peer := range p {
		a.Object(peer)
	}
}

// RequestPeer is bundle of peer ID, provided or
// determined addresses and net port
type RequestPeer struct {
	ID   PeerID
	Port uint16
	RequestAddresses
}

// Peers constructs array of Peer-s with the same ID and Port
// for every RequestAddress array.
func (rp RequestPeer) Peers() (peers Peers) {
	for _, a := range rp.RequestAddresses {
		peers = append(peers, Peer{
			ID:       rp.ID,
			AddrPort: netip.AddrPortFrom(a.Addr, rp.Port),
		})
	}
	return
}

// MarshalZerologObject writes fields into zerolog event
func (rp RequestPeer) MarshalZerologObject(e *zerolog.Event) {
	e.Stringer("id", rp.ID).
		Array("addresses", rp.RequestAddresses).
		Uint16("port", rp.Port)
}

// AnnounceRequest represents the parsed parameters from an announce request.
type AnnounceRequest struct {
	Event           Event
	InfoHash        InfoHash
	Compact         bool
	EventProvided   bool
	NumWantProvided bool
	NumWant         uint32
	Left            uint64
	Downloaded      uint64
	Uploaded        uint64

	RequestPeer
	Params
}

// MarshalZerologObject writes fields into zerolog event
func (r AnnounceRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Stringer("event", r.Event).
		Stringer("infoHash", r.InfoHash).
		Bool("compact", r.Compact).
		Bool("eventProvided", r.EventProvided).
		Bool("numWantProvided", r.NumWantProvided).
		Uint32("numWant", r.NumWant).
		Uint64("left", r.Left).
		Uint64("downloaded", r.Downloaded).
		Uint64("uploaded", r.Uploaded).
		Object("peers", r.RequestPeer).
		Object("params", r.Params)
}

// AnnounceResponse represents the parameters used to create an announce
// response.
type AnnounceResponse struct {
	Compact     bool
	Complete    uint32
	Incomplete  uint32
	Interval    time.Duration
	MinInterval time.Duration
	IPv4Peers   Peers
	IPv6Peers   Peers
}

// MarshalZerologObject writes fields into zerolog event
func (r AnnounceResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Bool("compact", r.Compact).
		Uint32("complete", r.Complete).
		Uint32("incomplete", r.Incomplete).
		Dur("interval", r.Interval).
		Dur("minInterval", r.MinInterval).
		Array("ipv4Peers", r.IPv4Peers).
		Array("ipv6Peers", r.IPv6Peers)
}

// InfoHashes wrapper of array of InfoHash-es
type InfoHashes []InfoHash

// MarshalZerologArray writes array elements to zerolog event
func (i InfoHashes) MarshalZerologArray(a *zerolog.Array) {
	for _, ih := range i {
		a.Str(ih.String())
	}
}

// ScrapeRequest represents the parsed parameters from a scrape request.
type ScrapeRequest struct {
	// RequestAddresses not used in internal logic,
	// but MAY be used in middleware (per-ip block etc.)
	RequestAddresses
	InfoHashes InfoHashes
	Params     Params
}

// MarshalZerologObject writes fields into zerolog event
func (r ScrapeRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Array("addresses", r.RequestAddresses).
		Array("infoHashes", r.InfoHashes).
		Object("params", r.Params)
}

// Scrapes wrapper of array of Scrape-s
type Scrapes []Scrape

// MarshalZerologArray writes array elements to zerolog event
func (s Scrapes) MarshalZerologArray(a *zerolog.Array) {
	for _, scrape := range s {
		a.Object(scrape)
	}
}

// ScrapeResponse represents the parameters used to create a scrape response.
//
// The Scrapes must be in the same order as the InfoHashes in the corresponding
// ScrapeRequest.
type ScrapeResponse struct {
	Files Scrapes
}

// MarshalZerologObject writes fields into zerolog event
func (sr ScrapeResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Array("scrapes", sr.Files)
}
