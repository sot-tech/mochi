// Package test contains storage tests.
// Not used in production.
package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/storage"
)

// PeerEqualityFunc is the boolean function to use to check two Peers for
// equality.
// Depending on the implementation of the Storage, this can be changed to
// use (Peer).EqualEndpoint instead.
var PeerEqualityFunc = func(p1, p2 bittorrent.Peer) bool { return p1.Equal(p2) }

type testHolder struct {
	st storage.Storage
}

type hashPeer struct {
	ih   bittorrent.InfoHash
	peer bittorrent.Peer
}

func (th *testHolder) DeleteSeeder(t *testing.T) {
	for _, c := range testData {
		err := th.st.DeleteSeeder(c.ih, c.peer)
		require.Equal(t, storage.ErrResourceDoesNotExist, err)
	}
}

func (th *testHolder) PutLeecher(t *testing.T) {
	for _, c := range testData {
		peer := v4Peer
		if c.peer.Addr().Is6() {
			peer = v6Peer
		}
		err := th.st.PutLeecher(c.ih, peer)
		require.Nil(t, err)
	}
}

func (th *testHolder) DeleteLeecher(t *testing.T) {
	for _, c := range testData {
		err := th.st.DeleteLeecher(c.ih, c.peer)
		require.Equal(t, storage.ErrResourceDoesNotExist, err)
	}
}

func (th *testHolder) AnnouncePeers(t *testing.T) {
	for _, c := range testData {
		peer := v4Peer
		if c.peer.Addr().Is6() {
			peer = v6Peer
		}
		_, err := th.st.AnnouncePeers(c.ih, false, 50, peer)
		require.Equal(t, storage.ErrResourceDoesNotExist, err)
	}
}

func (th *testHolder) ScrapeSwarm(t *testing.T) {
	for _, c := range testData {
		scrape := th.st.ScrapeSwarm(c.ih, c.peer)
		require.Equal(t, uint32(0), scrape.Complete)
		require.Equal(t, uint32(0), scrape.Incomplete)
		require.Equal(t, uint32(0), scrape.Snatches)
	}
}

func (th *testHolder) LeecherPutAnnounceDeleteAnnounce(t *testing.T) {
	for _, c := range testData {
		peer := v4Peer
		if c.peer.Addr().Is6() {
			peer = v6Peer
		}
		err := th.st.PutLeecher(c.ih, c.peer)
		require.Nil(t, err)

		peers, err := th.st.AnnouncePeers(c.ih, true, 50, peer)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		// non-seeder announce should still return the leecher
		peers, err = th.st.AnnouncePeers(c.ih, false, 50, peer)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		scrape := th.st.ScrapeSwarm(c.ih, c.peer)
		require.Equal(t, uint32(2), scrape.Incomplete)
		require.Equal(t, uint32(0), scrape.Complete)

		err = th.st.DeleteLeecher(c.ih, c.peer)
		require.Nil(t, err)

		peers, err = th.st.AnnouncePeers(c.ih, true, 50, peer)
		require.Nil(t, err)
		require.False(t, containsPeer(peers, c.peer))
	}
}

func (th *testHolder) SeederPutAnnounceDeleteAnnounce(t *testing.T) {
	for _, c := range testData {
		peer := v4Peer
		if c.peer.Addr().Is6() {
			peer = v6Peer
		}
		err := th.st.PutSeeder(c.ih, c.peer)
		require.Nil(t, err)

		// Should be leecher to see the seeder
		peers, err := th.st.AnnouncePeers(c.ih, false, 50, peer)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		scrape := th.st.ScrapeSwarm(c.ih, c.peer)
		require.Equal(t, uint32(1), scrape.Incomplete)
		require.Equal(t, uint32(1), scrape.Complete)

		err = th.st.DeleteSeeder(c.ih, c.peer)
		require.Nil(t, err)

		peers, err = th.st.AnnouncePeers(c.ih, false, 50, peer)
		require.Nil(t, err)
		require.False(t, containsPeer(peers, c.peer))
	}
}

func (th *testHolder) LeecherPutGraduateAnnounceDeleteAnnounce(t *testing.T) {
	for _, c := range testData {
		peer := v4Peer
		if c.peer.Addr().Is6() {
			peer = v6Peer
		}
		err := th.st.PutLeecher(c.ih, c.peer)
		require.Nil(t, err)

		err = th.st.GraduateLeecher(c.ih, c.peer)
		require.Nil(t, err)

		// Has to be leecher to see the graduated seeder
		peers, err := th.st.AnnouncePeers(c.ih, false, 50, peer)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		// Deleting the Peer as a Leecher should have no effect
		err = th.st.DeleteLeecher(c.ih, c.peer)
		require.Equal(t, storage.ErrResourceDoesNotExist, err)

		// Verify it's still there
		peers, err = th.st.AnnouncePeers(c.ih, false, 50, peer)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		// Clean up
		err = th.st.DeleteLeecher(c.ih, peer)
		require.Nil(t, err)

		// Test ErrDNE for missing leecher
		err = th.st.DeleteLeecher(c.ih, peer)
		require.Equal(t, storage.ErrResourceDoesNotExist, err)

		err = th.st.DeleteSeeder(c.ih, c.peer)
		require.Nil(t, err)

		err = th.st.DeleteSeeder(c.ih, c.peer)
		require.Equal(t, storage.ErrResourceDoesNotExist, err)
	}
}

func (th *testHolder) CustomPutContainsLoadDelete(t *testing.T) {
	for _, c := range testData {
		err := th.st.Put("test", storage.Entry{Key: c.peer.String(), Value: c.ih.RawString()})
		require.Nil(t, err)

		// check if exist in ctx we put
		contains, err := th.st.Contains("test", c.peer.String())
		require.Nil(t, err)
		require.True(t, contains)

		// check if not exist in another ctx
		contains, err = th.st.Contains("", c.peer.String())
		require.Nil(t, err)
		require.False(t, contains)

		// check value and type in ctx we put
		out, err := th.st.Load("test", c.peer.String())
		require.Nil(t, err)
		ih, err := bittorrent.NewInfoHash(out)
		require.Nil(t, err)
		require.Equal(t, c.ih, ih)

		// check value is nil in another ctx
		dummy, err := th.st.Load("", c.peer.String())
		require.Nil(t, err)
		require.Nil(t, dummy)

		err = th.st.Delete("test", c.peer.String())
		require.Nil(t, err)

		contains, err = th.st.Contains("peers", c.peer.String())
		require.Nil(t, err)
		require.False(t, contains)
	}
}

func (th *testHolder) CustomBulkPutContainsLoadDelete(t *testing.T) {
	pairs := make([]storage.Entry, 0, len(testData))
	keys := make([]string, 0, len(testData))
	for _, c := range testData {
		key := c.peer.String()
		keys = append(keys, key)
		pairs = append(pairs, storage.Entry{
			Key:   key,
			Value: c.ih.RawString(),
		})
	}
	err := th.st.BulkPut("test", pairs...)
	require.Nil(t, err)

	// check if exist in ctx we put
	for _, k := range keys {
		contains, err := th.st.Contains("test", k)
		require.Nil(t, err)
		require.True(t, contains)
	}

	// check value and type in ctx we put
	for _, p := range pairs {
		out, _ := th.st.Load("test", p.Key)
		ih, err := bittorrent.NewInfoHash(out)
		require.Nil(t, err)
		require.Equal(t, p.Value, ih.RawString())
	}

	err = th.st.Delete("test", keys...)
	require.Nil(t, err)

	for _, k := range keys {
		contains, err := th.st.Contains("test", k)
		require.Nil(t, err)
		require.False(t, contains)
	}
}

func (th *testHolder) GC(t *testing.T) {
	for _, c := range testData {
		require.Nil(t, th.st.PutSeeder(c.ih, c.peer))
		require.Nil(t, th.st.PutSeeder(c.ih, v4Peer))
		require.Nil(t, th.st.PutSeeder(c.ih, v6Peer))
	}
	th.st.GC(time.Now().Add(time.Hour))
	for _, c := range testData {
		_, err := th.st.AnnouncePeers(c.ih, false, 100, v4Peer)
		require.Equal(t, storage.ErrResourceDoesNotExist, err)
	}
}

// RunTests tests a Storage implementation against the interface.
func RunTests(t *testing.T, p storage.Storage) {
	th := testHolder{st: p}

	// Test ErrDNE for non-existent swarms.
	t.Run("DeleteLeecher", th.DeleteLeecher)
	t.Run("DeleteSeeder", th.DeleteSeeder)
	t.Run("AnnouncePeers", th.AnnouncePeers)

	// Test empty scrape response for non-existent swarms.
	t.Run("ScrapeSwarm", th.ScrapeSwarm)

	// Insert dummy Peer to keep swarm active
	// Has the same address family as c.peer
	t.Run("PutLeecher", th.PutLeecher)

	// Test ErrDNE for non-existent seeder.
	t.Run("DeleteSeeder", th.DeleteSeeder)

	// Test PutLeecher -> Announce -> DeleteLeecher -> Announce
	t.Run("LeecherPutAnnounceDeleteAnnounce", th.LeecherPutAnnounceDeleteAnnounce)

	// Test PutSeeder -> Announce -> DeleteSeeder -> Announce
	t.Run("SeederPutAnnounceDeleteAnnounce", th.SeederPutAnnounceDeleteAnnounce)

	// Test PutLeecher -> Graduate -> Announce -> DeleteLeecher -> Announce
	t.Run("LeecherPutGraduateAnnounceDeleteAnnounce", th.LeecherPutGraduateAnnounceDeleteAnnounce)

	t.Run("CustomPutContainsLoadDelete", th.CustomPutContainsLoadDelete)
	t.Run("CustomBulkPutContainsLoadDelete", th.CustomBulkPutContainsLoadDelete)

	t.Run("GC", th.GC)

	e := th.st.Stop()
	require.Nil(t, <-e)
}

func containsPeer(peers []bittorrent.Peer, p bittorrent.Peer) bool {
	for _, peer := range peers {
		if PeerEqualityFunc(peer, p) {
			return true
		}
	}
	return false
}
