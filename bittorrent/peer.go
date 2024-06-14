// Package bittorrent implements all of the abstractions used to decouple the
// protocol of a BitTorrent tracker from the logic of handling Announces and
// Scrapes.
package bittorrent

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"

	"github.com/rs/zerolog"
	"github.com/sot-tech/mochi/pkg/str2bytes"
)

// PeerIDLen is length of peer id field in bytes
const PeerIDLen = 20

// PeerID represents a peer ID.
type PeerID [PeerIDLen]byte

// ErrInvalidPeerIDSize holds error about invalid PeerID size
var ErrInvalidPeerIDSize = fmt.Errorf("peer ID must be %d bytes", PeerIDLen)

var zeroPeerID PeerID

// NewPeerID creates a PeerID from a byte slice.
func NewPeerID(b []byte) (PeerID, error) {
	if len(b) != PeerIDLen {
		return zeroPeerID, ErrInvalidPeerIDSize
	}
	return PeerID(b), nil
}

// Bytes returns slice of bytes represents this PeerID
func (p PeerID) Bytes() []byte {
	return p[:]
}

// String implements fmt.Stringer, returning the base16 encoded PeerID.
func (p PeerID) String() string {
	return hex.EncodeToString(p.Bytes())
}

// RawString returns a 20-byte string of the raw bytes of the ID.
func (p PeerID) RawString() string {
	return str2bytes.BytesToString(p.Bytes())
}

// InfoHash represents an infohash.
type InfoHash string

const (
	// InfoHashV1Len is the same as sha1.Size
	InfoHashV1Len = sha1.Size
	// InfoHashV2Len ... sha256.Size
	InfoHashV2Len = sha256.Size
)

// ErrInvalidHashSize holds error about invalid InfoHash size
var ErrInvalidHashSize = fmt.Errorf("info hash must be either %d (for torrent V1) or %d (V2) bytes or same sizes x2 (if HEX encoded)", InfoHashV1Len, InfoHashV2Len)

// NewInfoHash creates an InfoHash from raw/hex byte slice.
func NewInfoHash(data []byte) (InfoHash, error) {
	var ih InfoHash
	switch l := len(data); l {
	case InfoHashV1Len, InfoHashV2Len:
		ih = InfoHash(data)
	case InfoHashV1Len * 2, InfoHashV2Len * 2:
		bb := make([]byte, l/2)
		if _, err := hex.Decode(bb, data); err != nil {
			return "", err
		}
		ih = InfoHash(str2bytes.BytesToString(bb))
	default:
		return "", ErrInvalidHashSize
	}
	return ih, nil
}

// NewInfoHashString creates an InfoHash from raw/hex string.
func NewInfoHashString(data string) (InfoHash, error) {
	return NewInfoHash(str2bytes.StringToBytes(data))
}

// TruncateV1 returns truncated to 20-bytes length array of the corresponding InfoHash.
// If InfoHash is V2 (32 bytes), it will be truncated to 20 bytes
// according to BEP52.
func (i InfoHash) TruncateV1() InfoHash {
	if len(i) == InfoHashV2Len {
		return i[:InfoHashV1Len]
	}
	return i
}

// Bytes returns slice of bytes represents this InfoHash
func (i InfoHash) Bytes() []byte {
	return str2bytes.StringToBytes(string(i))
}

// String implements fmt.Stringer, returning the base16 encoded InfoHash.
func (i InfoHash) String() string {
	return hex.EncodeToString(i.Bytes())
}

// RawString returns a string of the raw bytes of the InfoHash.
func (i InfoHash) RawString() string {
	return string(i)
}

// Peer represents the connection details of a peer that is returned in an
// announce response.
type Peer struct {
	ID PeerID
	netip.AddrPort
}

// Addr returns unmapped peer's IP address
func (p Peer) Addr() netip.Addr {
	return p.AddrPort.Addr().Unmap()
}

// MarshalZerologObject writes fields into zerolog event
func (p Peer) MarshalZerologObject(e *zerolog.Event) {
	e.Stringer("id", p.ID).
		Stringer("addr", p.Addr()).
		Uint16("port", p.Port())
}

// ClientError represents an error that should be exposed to the client over
// the BitTorrent protocol implementation.
type ClientError string

// Error implements the error interface for ClientError.
func (c ClientError) Error() string { return string(c) }
