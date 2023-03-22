// Package clientapproval XXX: implementation is broken, client ID is NOT 6 static bytes
// refer:
// - https://wiki.theory.org/BitTorrentSpecification#peer_id
// - https://github.com/webtorrent/bittorrent-peerid/blob/master/lib/utils.js
package clientapproval

import (
	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/str2bytes"
)

// ClientID represents the part of a PeerID that identifies a Peer's client
// software.
type ClientID [6]byte

// NewClientID parses a ClientID from a PeerID.
func NewClientID(pid bittorrent.PeerID) ClientID {
	var cid ClientID
	if pid[0] == '-' {
		copy(cid[:], pid[1:7])
	} else {
		copy(cid[:], pid[:6])
	}

	return cid
}

func (cid ClientID) String() string {
	return str2bytes.BytesToString(cid[:])
}
