// Package test contains generated data for storage tests.
// Not used in production.
package test

import (
	"crypto/rand"
	"net/netip"

	"github.com/sot-tech/mochi/bittorrent"
)

var (
	testIh1, testIh2                                   bittorrent.InfoHash
	testPeerID0, testPeerID1, testPeerID2, testPeerID3 bittorrent.PeerID
	testData                                           []hashPeer
	v4Peer, v6Peer                                     bittorrent.Peer
)

func randIH(v2 bool) (ih bittorrent.InfoHash) {
	var b []byte
	if v2 {
		b = make([]byte, bittorrent.InfoHashV2Len)
	} else {
		b = make([]byte, bittorrent.InfoHashV1Len)
	}
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	ih, _ = bittorrent.NewInfoHash(b)
	return
}

func randPeerID() (ih bittorrent.PeerID) {
	b := make([]byte, bittorrent.PeerIDLen)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	ih, _ = bittorrent.NewPeerID(b)
	return
}

func init() {
	testIh1 = randIH(false)
	testIh2 = randIH(true)
	testPeerID0 = randPeerID()
	testPeerID1 = randPeerID()
	testPeerID2 = randPeerID()
	testPeerID3 = randPeerID()
	testData = []hashPeer{
		{
			testIh1,
			bittorrent.Peer{ID: testPeerID0, AddrPort: netip.MustParseAddrPort("1.1.1.1:1")},
		},
		{
			testIh2,
			bittorrent.Peer{ID: testPeerID1, AddrPort: netip.MustParseAddrPort("[abab::0001]:2")},
		},
	}

	v4Peer = bittorrent.Peer{ID: testPeerID2, AddrPort: netip.MustParseAddrPort("99.99.99.99:9994")}
	v6Peer = bittorrent.Peer{ID: testPeerID3, AddrPort: netip.MustParseAddrPort("[fc00::0001]:9996")}
}
