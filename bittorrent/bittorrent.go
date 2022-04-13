// Package bittorrent implements all of the abstractions used to decouple the
// protocol of a BitTorrent tracker from the logic of handling Announces and
// Scrapes.
package bittorrent

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"

	"github.com/sot-tech/mochi/pkg/log"
)

// PeerIDLen is length of peer id field in bytes
const PeerIDLen = 20

// PeerID represents a peer ID.
type PeerID [PeerIDLen]byte

// ErrInvalidPeerIDSize holds error about invalid PeerID size
var ErrInvalidPeerIDSize = fmt.Errorf("peer ID must be %d bytes", PeerIDLen)

// NewPeerID creates a PeerID from a byte slice.
//
// It panics if b is not 20 bytes long.
func NewPeerID(b []byte) (PeerID, error) {
	var p PeerID
	if len(b) != PeerIDLen {
		return p, ErrInvalidPeerIDSize
	}
	copy(p[:], b)
	return p, nil
}

// String implements fmt.Stringer, returning the base16 encoded PeerID.
func (p PeerID) String() string {
	return hex.EncodeToString(p[:])
}

// RawString returns a 20-byte string of the raw bytes of the ID.
func (p PeerID) RawString() string {
	return string(p[:])
}

// InfoHash represents an infohash.
type InfoHash string

const (
	// InfoHashV1Len is the same as sha1.Size
	InfoHashV1Len = sha1.Size
	// InfoHashV2Len ... sha256.Size
	InfoHashV2Len = sha256.Size
	// NoneInfoHash dummy invalid InfoHash
	NoneInfoHash InfoHash = ""
)

var (
	// ErrInvalidHashType holds error about invalid InfoHash input type
	ErrInvalidHashType = errors.New("info hash must be provided as byte slice or raw/hex string")
	// ErrInvalidHashSize holds error about invalid InfoHash size
	ErrInvalidHashSize = fmt.Errorf("info hash must be either %d (for torrent V1) or %d (V2) bytes", InfoHashV1Len, InfoHashV2Len)
)

// TruncateV1 returns truncated to 20-bytes length array of the corresponding InfoHash.
// If InfoHash is V2 (32 bytes), it will be truncated to 20 bytes
// according to BEP52.
func (i InfoHash) TruncateV1() InfoHash {
	if len(i) == InfoHashV2Len {
		return i[:InfoHashV1Len]
	}
	return i
}

// NewInfoHash creates an InfoHash from a byte slice or raw/hex string.
func NewInfoHash(data any) (InfoHash, error) {
	if data == nil {
		return NoneInfoHash, ErrInvalidHashType
	}
	var ba []byte
	switch t := data.(type) {
	case [InfoHashV1Len]byte:
		ba = t[:]
	case [InfoHashV2Len]byte:
		ba = t[:]
	case []byte:
		ba = t
	case string:
		l := len(t)
		if l == InfoHashV1Len*2 || l == InfoHashV2Len*2 {
			var err error
			if ba, err = hex.DecodeString(t); err != nil {
				return NoneInfoHash, err
			}
		} else {
			ba = []byte(t)
		}
	}
	l := len(ba)
	if l != InfoHashV1Len && l != InfoHashV2Len {
		return NoneInfoHash, ErrInvalidHashSize
	}
	return InfoHash(ba), nil
}

// String implements fmt.Stringer, returning the base16 encoded InfoHash.
func (i InfoHash) String() string {
	return hex.EncodeToString([]byte(i))
}

// RawString returns a string of the raw bytes of the InfoHash.
func (i InfoHash) RawString() string {
	return string(i)
}

// AnnounceRequest represents the parsed parameters from an announce request.
type AnnounceRequest struct {
	Event           Event
	InfoHash        InfoHash
	Compact         bool
	EventProvided   bool
	NumWantProvided bool
	IPProvided      bool
	NumWant         uint32
	Left            uint64
	Downloaded      uint64
	Uploaded        uint64

	Peer
	Params
}

// LogFields renders the current response as a set of log fields.
func (r AnnounceRequest) LogFields() log.Fields {
	return log.Fields{
		"event":           r.Event,
		"infoHash":        r.InfoHash,
		"compact":         r.Compact,
		"eventProvided":   r.EventProvided,
		"numWantProvided": r.NumWantProvided,
		"ipProvided":      r.IPProvided,
		"numWant":         r.NumWant,
		"left":            r.Left,
		"downloaded":      r.Downloaded,
		"uploaded":        r.Uploaded,
		"peer":            r.Peer,
		"params":          r.Params,
	}
}

// AnnounceResponse represents the parameters used to create an announce
// response.
type AnnounceResponse struct {
	Compact     bool
	Complete    uint32
	Incomplete  uint32
	Interval    time.Duration
	MinInterval time.Duration
	IPv4Peers   []Peer
	IPv6Peers   []Peer
}

// LogFields renders the current response as a set of log fields.
func (r AnnounceResponse) LogFields() log.Fields {
	return log.Fields{
		"compact":     r.Compact,
		"complete":    r.Complete,
		"interval":    r.Interval,
		"minInterval": r.MinInterval,
		"ipv4Peers":   r.IPv4Peers,
		"ipv6Peers":   r.IPv6Peers,
	}
}

// ScrapeRequest represents the parsed parameters from a scrape request.
type ScrapeRequest struct {
	AddressFamily AddressFamily
	InfoHashes    []InfoHash
	Params        Params
}

// LogFields renders the current response as a set of log fields.
func (r ScrapeRequest) LogFields() log.Fields {
	return log.Fields{
		"addressFamily": r.AddressFamily,
		"infoHashes":    r.InfoHashes,
		"params":        r.Params,
	}
}

// ScrapeResponse represents the parameters used to create a scrape response.
//
// The Scrapes must be in the same order as the InfoHashes in the corresponding
// ScrapeRequest.
type ScrapeResponse struct {
	Files []Scrape
}

// LogFields renders the current response as a set of Logrus fields.
func (sr ScrapeResponse) LogFields() log.Fields {
	return log.Fields{
		"files": sr.Files,
	}
}

// Scrape represents the state of a swarm that is returned in a scrape response.
type Scrape struct {
	InfoHash   InfoHash
	Snatches   uint32
	Complete   uint32
	Incomplete uint32
}

// AddressFamily is the address family of an IP address.
type AddressFamily uint8

func (af AddressFamily) String() string {
	switch af {
	case IPv4:
		return "IPv4"
	case IPv6:
		return "IPv6"
	default:
		return "<unknown>"
	}
}

// AddressFamily constants.
const (
	IPv4 AddressFamily = iota
	IPv6
)

// IP is a net.IP with an AddressFamily.
type IP struct {
	net.IP
	AddressFamily
}

func (ip IP) String() string {
	return ip.IP.String()
}

// Peer represents the connection details of a peer that is returned in an
// announce response.
type Peer struct {
	ID   PeerID
	IP   IP
	Port uint16
}

// PeerMinimumLen is the least allowed length of string serialized Peer
const PeerMinimumLen = PeerIDLen + 2 + net.IPv4len

var (
	// ErrInvalidAddressFamily holds error about invalid address family
	ErrInvalidAddressFamily = fmt.Errorf("address family must be %d(IPv4) or %d(IPv6)", IPv4, IPv6)

	// ErrInvalidPeerDataSize holds error about invalid Peer data size
	ErrInvalidPeerDataSize = fmt.Errorf("invalid peer data it must be at least %d bytes (InfoHash + Port + IPv4)", PeerMinimumLen)
)

// NewPeer constructs Peer from serialized by Peer.RawString data: PeerID[20by]Port[2by]net.IP[4/16by]
func NewPeer(data string) (Peer, error) {
	var peer Peer
	if len(data) < PeerMinimumLen {
		return peer, ErrInvalidPeerDataSize
	}
	peerID, err := NewPeerID([]byte(data[:PeerIDLen]))
	if err == nil {
		peer = Peer{
			ID:   peerID,
			Port: binary.BigEndian.Uint16([]byte(data[PeerIDLen : PeerIDLen+2])),
			IP:   IP{IP: net.IP(data[PeerIDLen+2:])},
		}

		if ip := peer.IP.To4(); ip != nil {
			peer.IP.IP = ip
			peer.IP.AddressFamily = IPv4
		} else if len(peer.IP.IP) == net.IPv6len { // implies toReturn.IP.To4() == nil
			peer.IP.AddressFamily = IPv6
		} else {
			err = ErrInvalidAddressFamily
		}
	}

	return peer, err
}

// String implements fmt.Stringer to return a human-readable representation.
// The string will have the format <PeerID>@[<IP>]:<port>, for example
// "0102030405060708090a0b0c0d0e0f1011121314@[10.11.12.13]:1234"
func (p Peer) String() string {
	return fmt.Sprintf("%s@[%s]:%d", p.ID.String(), p.IP.String(), p.Port)
}

// RawString generates concatenation of PeerID, net port and IP-address
func (p Peer) RawString() string {
	b := make([]byte, PeerIDLen+2+len(p.IP.IP))
	copy(b[:PeerIDLen], p.ID[:])
	binary.BigEndian.PutUint16(b[PeerIDLen:PeerIDLen+2], p.Port)
	copy(b[PeerIDLen+2:], p.IP.IP)
	return string(b)
}

// LogFields renders the current peer as a set of Logrus fields.
func (p Peer) LogFields() log.Fields {
	return log.Fields{
		"ID":   p.ID,
		"IP":   p.IP,
		"port": p.Port,
	}
}

// Equal reports whether p and x are the same.
func (p Peer) Equal(x Peer) bool { return p.EqualEndpoint(x) && p.ID == x.ID }

// EqualEndpoint reports whether p and x have the same endpoint.
func (p Peer) EqualEndpoint(x Peer) bool { return p.Port == x.Port && p.IP.Equal(x.IP.IP) }

// ClientError represents an error that should be exposed to the client over
// the BitTorrent protocol implementation.
type ClientError string

// Error implements the error interface for ClientError.
func (c ClientError) Error() string { return string(c) }
