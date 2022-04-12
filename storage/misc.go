package storage

import (
	"encoding/binary"
	"github.com/sot-tech/mochi/bittorrent"
	"net"
)

// Pair - some key-value pair, used for BulkPut
type Pair struct {
	Left, Right any
}

// SerializedPeer concatenation of PeerID, net port and IP-address
type SerializedPeer string

// NewSerializedPeer builds SerializedPeer from bittorrent.Peer
func NewSerializedPeer(p bittorrent.Peer) SerializedPeer {
	b := make([]byte, bittorrent.PeerIDLen+2+len(p.IP.IP))
	copy(b[:bittorrent.PeerIDLen], p.ID[:])
	binary.BigEndian.PutUint16(b[bittorrent.PeerIDLen:bittorrent.PeerIDLen+2], p.Port)
	copy(b[bittorrent.PeerIDLen+2:], p.IP.IP)

	return SerializedPeer(b)
}

// ToPeer parses SerializedPeer to bittorrent.Peer
func (pk SerializedPeer) ToPeer() bittorrent.Peer {
	peerID, err := bittorrent.NewPeerID([]byte(pk[:bittorrent.PeerIDLen]))
	if err != nil {
		panic(err)
	}
	peer := bittorrent.Peer{
		ID:   peerID,
		Port: binary.BigEndian.Uint16([]byte(pk[bittorrent.PeerIDLen : bittorrent.PeerIDLen+2])),
		IP:   bittorrent.IP{IP: net.IP(pk[bittorrent.PeerIDLen+2:])}}

	if ip := peer.IP.To4(); ip != nil {
		peer.IP.IP = ip
		peer.IP.AddressFamily = bittorrent.IPv4
	} else if len(peer.IP.IP) == net.IPv6len { // implies toReturn.IP.To4() == nil
		peer.IP.AddressFamily = bittorrent.IPv6
	} else {
		panic("IP is neither v4 nor v6")
	}

	return peer
}
