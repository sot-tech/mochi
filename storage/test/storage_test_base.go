// Package test contains storage tests.
// Not used in production.
package test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
)

const kvStoreCtx = "test"

func init() {
	_ = log.ConfigureLogger("", "warn", false, false)
}

// PeerEqualityFunc is the boolean function to use to check two Peers for
// equality.
// Depending on the implementation of the PeerStorage, this can be changed to
// use (Peer).EqualEndpoint instead.
var PeerEqualityFunc = func(p1, p2 bittorrent.Peer) bool {
	return p1.Port() == p2.Port() &&
		p1.Addr().Compare(p1.Addr()) == 0 &&
		p1.ID == p2.ID
}

type testHolder struct {
	st storage.PeerStorage
}

type hashPeer struct {
	ih   bittorrent.InfoHash
	peer bittorrent.Peer
}

func (th *testHolder) DeleteSeeder(t *testing.T) {
	for _, c := range testData {
		err := th.st.DeleteSeeder(context.TODO(), c.ih, c.peer)
		if errors.Is(err, storage.ErrResourceDoesNotExist) {
			err = nil
		}
		require.Nil(t, err)
	}
}

func (th *testHolder) PutLeecher(t *testing.T) {
	for _, c := range testData {
		peer := v4Peer
		if c.peer.Addr().Is6() {
			peer = v6Peer
		}
		err := th.st.PutLeecher(context.TODO(), c.ih, peer)
		require.Nil(t, err)
	}
}

func (th *testHolder) DeleteLeecher(t *testing.T) {
	for _, c := range testData {
		err := th.st.DeleteLeecher(context.TODO(), c.ih, c.peer)
		if errors.Is(err, storage.ErrResourceDoesNotExist) {
			err = nil
		}
		require.Nil(t, err)
	}
}

func (th *testHolder) AnnouncePeers(t *testing.T) {
	for _, c := range testData {
		_, err := th.st.AnnouncePeers(context.TODO(), c.ih, false, 50, c.peer.Addr().Is6())
		if errors.Is(err, storage.ErrResourceDoesNotExist) {
			err = nil
		}
		require.Nil(t, err)
	}
}

func (th *testHolder) ScrapeSwarm(t *testing.T) {
	for _, c := range testData {
		l, s, n := th.st.ScrapeSwarm(context.TODO(), c.ih)
		require.Equal(t, uint32(0), s)
		require.Equal(t, uint32(0), l)
		require.Equal(t, uint32(0), n)
	}
}

func (th *testHolder) LeecherPutAnnounceDeleteAnnounce(t *testing.T) {
	for _, c := range testData {
		isV6 := c.peer.Addr().Is6()
		err := th.st.PutLeecher(context.TODO(), c.ih, c.peer)
		require.Nil(t, err)

		peers, err := th.st.AnnouncePeers(context.TODO(), c.ih, true, 50, isV6)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		// non-seeder announce should still return the leecher
		peers, err = th.st.AnnouncePeers(context.TODO(), c.ih, false, 50, isV6)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		l, s, _ := th.st.ScrapeSwarm(context.TODO(), c.ih)
		require.Equal(t, uint32(2), l)
		require.Equal(t, uint32(0), s)

		err = th.st.DeleteLeecher(context.TODO(), c.ih, c.peer)
		require.Nil(t, err)

		peers, err = th.st.AnnouncePeers(context.TODO(), c.ih, true, 50, isV6)
		if errors.Is(err, storage.ErrResourceDoesNotExist) {
			err = nil
		}
		require.Nil(t, err)
		require.False(t, containsPeer(peers, c.peer))
	}
}

func (th *testHolder) SeederPutAnnounceDeleteAnnounce(t *testing.T) {
	for _, c := range testData {
		isV6 := c.peer.Addr().Is6()
		err := th.st.PutSeeder(context.TODO(), c.ih, c.peer)
		require.Nil(t, err)

		// Should be leecher to see the seeder
		peers, err := th.st.AnnouncePeers(context.TODO(), c.ih, false, 50, isV6)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		l, s, _ := th.st.ScrapeSwarm(context.TODO(), c.ih)
		require.Equal(t, uint32(1), l)
		require.Equal(t, uint32(1), s)

		err = th.st.DeleteSeeder(context.TODO(), c.ih, c.peer)
		require.Nil(t, err)

		peers, err = th.st.AnnouncePeers(context.TODO(), c.ih, false, 50, isV6)
		if errors.Is(err, storage.ErrResourceDoesNotExist) {
			err = nil
		}
		require.Nil(t, err)
		require.False(t, containsPeer(peers, c.peer))
	}
}

func (th *testHolder) LeecherPutGraduateAnnounceDeleteAnnounce(t *testing.T) {
	for _, c := range testData {
		isV6 := c.peer.Addr().Is6()
		peer := v4Peer
		if isV6 {
			peer = v6Peer
		}
		err := th.st.PutLeecher(context.TODO(), c.ih, c.peer)
		require.Nil(t, err)

		err = th.st.GraduateLeecher(context.TODO(), c.ih, c.peer)
		require.Nil(t, err)

		// Has to be leecher to see the graduated seeder
		peers, err := th.st.AnnouncePeers(context.TODO(), c.ih, false, 50, isV6)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		// Deleting the Peer as a Leecher should have no effect
		err = th.st.DeleteLeecher(context.TODO(), c.ih, c.peer)
		if errors.Is(err, storage.ErrResourceDoesNotExist) {
			err = nil
		}
		require.Nil(t, err)

		// Verify it's still there
		peers, err = th.st.AnnouncePeers(context.TODO(), c.ih, false, 50, isV6)
		require.Nil(t, err)
		require.True(t, containsPeer(peers, c.peer))

		// Clean up
		err = th.st.DeleteLeecher(context.TODO(), c.ih, peer)
		require.Nil(t, err)

		// Test ErrDNE for missing leecher
		err = th.st.DeleteLeecher(context.TODO(), c.ih, peer)
		if errors.Is(err, storage.ErrResourceDoesNotExist) {
			err = nil
		}
		require.Nil(t, err)

		err = th.st.DeleteSeeder(context.TODO(), c.ih, c.peer)
		require.Nil(t, err)

		err = th.st.DeleteSeeder(context.TODO(), c.ih, c.peer)
		if errors.Is(err, storage.ErrResourceDoesNotExist) {
			err = nil
		}
		require.Nil(t, err)
	}
}

func (th *testHolder) CustomPutContainsLoadDelete(t *testing.T) {
	for _, c := range testData {
		err := th.st.Put(context.TODO(), kvStoreCtx, storage.Entry{Key: c.peer.String(), Value: []byte(c.ih.RawString())})
		require.Nil(t, err)

		// check if exist in ctx we put
		contains, err := th.st.Contains(context.TODO(), kvStoreCtx, c.peer.String())
		require.Nil(t, err)
		require.True(t, contains)

		// check if not exist in another ctx
		contains, err = th.st.Contains(context.TODO(), "", c.peer.String())
		require.Nil(t, err)
		require.False(t, contains)

		// check value and type in ctx we put
		out, err := th.st.Load(context.TODO(), kvStoreCtx, c.peer.String())
		require.Nil(t, err)
		ih, err := bittorrent.NewInfoHash(out)
		require.Nil(t, err)
		require.Equal(t, c.ih, ih)

		// check value is nil in another ctx
		dummy, err := th.st.Load(context.TODO(), "", c.peer.String())
		require.Nil(t, err)
		require.Nil(t, dummy)

		err = th.st.Delete(context.TODO(), kvStoreCtx, c.peer.String())
		require.Nil(t, err)

		contains, err = th.st.Contains(context.TODO(), "", c.peer.String())
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
			Value: []byte(c.ih.RawString()),
		})
	}
	err := th.st.Put(context.TODO(), kvStoreCtx, pairs...)
	require.Nil(t, err)

	// check if exist in ctx we put
	for _, k := range keys {
		contains, err := th.st.Contains(context.TODO(), kvStoreCtx, k)
		require.Nil(t, err)
		require.True(t, contains)
	}

	// check value and type in ctx we put
	for _, p := range pairs {
		out, _ := th.st.Load(context.TODO(), kvStoreCtx, p.Key)
		ih, err := bittorrent.NewInfoHash(out)
		require.Nil(t, err)
		require.Equal(t, p.Value, []byte(ih.RawString()))
	}

	err = th.st.Delete(context.TODO(), kvStoreCtx, keys...)
	require.Nil(t, err)

	for _, k := range keys {
		contains, err := th.st.Contains(context.TODO(), kvStoreCtx, k)
		require.Nil(t, err)
		require.False(t, contains)
	}
}

// RunTests tests a PeerStorage implementation against the interface.
func RunTests(t *testing.T, p storage.PeerStorage) {
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

	e := th.st.Close()
	require.Nil(t, e)
}

func containsPeer(peers []bittorrent.Peer, p bittorrent.Peer) bool {
	for _, peer := range peers {
		if PeerEqualityFunc(peer, p) {
			return true
		}
	}
	return false
}
