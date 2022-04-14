package test

import (
	"net/netip"

	"github.com/sot-tech/mochi/bittorrent"
)

var (
	testIh1, testIh2                                   bittorrent.InfoHash
	testPeerID0, testPeerID1, testPeerID2, testPeerID3 bittorrent.PeerID
	testData                                           []hashPeer
	v4Peer, v6Peer                                     bittorrent.Peer
)

func init() {
	testIh1, _ = bittorrent.NewInfoHash("00000000000000000001")
	testIh2, _ = bittorrent.NewInfoHash("00000000000000000002")
	testPeerID0, _ = bittorrent.NewPeerID([]byte("00000000000000000001"))
	testPeerID1, _ = bittorrent.NewPeerID([]byte("00000000000000000002"))
	testPeerID2, _ = bittorrent.NewPeerID([]byte("99999999999999999994"))
	testPeerID3, _ = bittorrent.NewPeerID([]byte("99999999999999999996"))
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
