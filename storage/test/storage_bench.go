// Package test contains storage benchmarks.
// Not used in production.
package test

import (
	"context"
	cr "crypto/rand"
	"math/rand"
	"net"
	"net/netip"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/sot-tech/mochi/bittorrent"
	// used for seeding global math.Rand
	_ "github.com/sot-tech/mochi/pkg/randseed"
	"github.com/sot-tech/mochi/storage"
)

const (
	ihCount    = 1000
	peersCount = 1000
)

type benchData struct {
	infoHashes [ihCount]bittorrent.InfoHash
	peers      [peersCount]bittorrent.Peer
}

func generateInfoHashes() (a [ihCount]bittorrent.InfoHash) {
	for i := range a {
		a[i] = randIH(i < ihCount/2)
	}
	return
}

func generatePeers() (a [peersCount]bittorrent.Peer) {
	for i := range a {
		var ip []byte
		if i%2 == 0 {
			ip = make([]byte, net.IPv4len)
		} else {
			ip = make([]byte, net.IPv6len)
		}
		if _, err := cr.Read(ip); err != nil {
			panic(err)
		}
		addr, ok := netip.AddrFromSlice(ip)
		if !ok {
			panic("unable to create ip from random bytes")
		}
		port := uint16(rand.Int63())
		a[i] = bittorrent.Peer{
			ID:       randPeerID(),
			AddrPort: netip.AddrPortFrom(addr, port),
		}
	}

	return
}

type (
	benchExecFunc           func(int, storage.PeerStorage, *benchData) error
	benchSetupFunc          func(storage.PeerStorage, *benchData) error
	benchStorageConstructor func() storage.PeerStorage
)

type benchHolder struct {
	st benchStorageConstructor
}

func (bh *benchHolder) runBenchmark(b *testing.B, parallel bool, sf benchSetupFunc, ef benchExecFunc) {
	ps := bh.st()
	bd := &benchData{generateInfoHashes(), generatePeers()}
	spacing := int32(ihCount / runtime.NumCPU())
	if sf != nil {
		err := sf(ps, bd)
		if err != nil {
			b.Fatal(err)
		}
	}
	offset := int32(0)

	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			i := int(atomic.AddInt32(&offset, spacing))
			for pb.Next() {
				err := ef(i, ps, bd)
				if err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			err := ef(i, ps, bd)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()

	if err := ps.Close(); err != nil {
		b.Fatal()
	}
}

// Nop executes a no-op for each iteration.
// It should produce the same results for each storage.PeerStorage.
// This can be used to get an estimate of the impact of the benchmark harness
// on benchmark results and an estimate of the general performance of the system
// benchmarked on.
//
// Nop can run in parallel.
func (bh *benchHolder) Nop(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.PeerStorage, bd *benchData) error {
		return nil
	})
}

// Put benchmarks the PutSeeder method of a storage.PeerStorage by repeatedly Putting the
// same Peer for the same InfoHash.
//
// Put can run in parallel.
func (bh *benchHolder) Put(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		return ps.PutSeeder(context.TODO(), bd.infoHashes[0], bd.peers[0])
	})
}

// Put1k benchmarks the PutSeeder method of a storage.PeerStorage by cycling through 1000
// Peers and Putting them into the swarm of one infohash.
//
// Put1k can run in parallel.
func (bh *benchHolder) Put1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		return ps.PutSeeder(context.TODO(), bd.infoHashes[0], bd.peers[i%peersCount])
	})
}

// Put1kInfoHash benchmarks the PutSeeder method of a storage.PeerStorage by cycling
// through 1000 infoHashes and putting the same peer into their swarms.
//
// Put1kInfoHash can run in parallel.
func (bh *benchHolder) Put1kInfoHash(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		return ps.PutSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[0])
	})
}

// Put1kInfoHash1k benchmarks the PutSeeder method of a storage.PeerStorage by cycling
// through 1000 infoHashes and 1000 Peers and calling Put with them.
//
// Put1kInfoHash1k can run in parallel.
func (bh *benchHolder) Put1kInfoHash1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[(i*3)%peersCount])
		return err
	})
}

// PutDelete benchmarks the PutSeeder and DeleteSeeder methods of a storage.PeerStorage by
// calling PutSeeder followed by DeleteSeeder for one Peer and one infohash.
//
// PutDelete can not run in parallel.
func (bh *benchHolder) PutDelete(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutSeeder(context.TODO(), bd.infoHashes[0], bd.peers[0])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(context.TODO(), bd.infoHashes[0], bd.peers[0])
	})
}

// PutDelete1k benchmarks the PutSeeder and DeleteSeeder methods in the same way
// PutDelete does, but with one from 1000 Peers per iteration.
//
// PutDelete1k can not run in parallel.
func (bh *benchHolder) PutDelete1k(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutSeeder(context.TODO(), bd.infoHashes[0], bd.peers[i%peersCount])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(context.TODO(), bd.infoHashes[0], bd.peers[i%peersCount])
	})
}

// PutDelete1kInfoHash behaves like PutDelete1k with 1000 infoHashes instead of
// 1000 Peers.
//
// PutDelete1kInfoHash can not run in parallel.
func (bh *benchHolder) PutDelete1kInfoHash(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[0])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[0])
	})
}

// PutDelete1kInfoHash1k behaves like PutDelete1k with 1000 infoHashes in
// addition to 1000 Peers.
//
// PutDelete1kInfoHash1k can not run in parallel.
func (bh *benchHolder) PutDelete1kInfoHash1k(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[(i*3)%peersCount])
		if err != nil {
			return err
		}
		err = ps.DeleteSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[(i*3)%peersCount])
		return err
	})
}

// DeleteNonexist benchmarks the DeleteSeeder method of a storage.PeerStorage by
// attempting to delete a Peer that is nonexistent.
//
// DeleteNonexist can run in parallel.
func (bh *benchHolder) DeleteNonexist(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_ = ps.DeleteSeeder(context.TODO(), bd.infoHashes[0], bd.peers[0])
		return nil
	})
}

// DeleteNonexist1k benchmarks the DeleteSeeder method of a storage.PeerStorage by
// attempting to delete one of 1000 nonexistent Peers.
//
// DeleteNonexist can run in parallel.
func (bh *benchHolder) DeleteNonexist1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_ = ps.DeleteSeeder(context.TODO(), bd.infoHashes[0], bd.peers[i%peersCount])
		return nil
	})
}

// DeleteNonexist1kInfoHash benchmarks the DeleteSeeder method of a storage.PeerStorage by
// attempting to delete one Peer from one of 1000 infoHashes.
//
// DeleteNonexist1kInfoHash can run in parallel.
func (bh *benchHolder) DeleteNonexist1kInfoHash(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_ = ps.DeleteSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[0])
		return nil
	})
}

// DeleteNonexist1kInfoHash1k benchmarks the Delete method of a storage.PeerStorage by
// attempting to delete one of 1000 Peers from one of 1000 InfoHashes.
//
// DeleteNonexist1kInfoHash1k can run in parallel.
func (bh *benchHolder) DeleteNonexist1kInfoHash1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_ = ps.DeleteSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[(i*3)%peersCount])
		return nil
	})
}

// GradNonexist benchmarks the GraduateLeecher method of a storage.PeerStorage by
// attempting to graduate a nonexistent Peer.
//
// GradNonexist can run in parallel.
func (bh *benchHolder) GradNonexist(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_ = ps.GraduateLeecher(context.TODO(), bd.infoHashes[0], bd.peers[0])
		return nil
	})
}

// GradNonexist1k benchmarks the GraduateLeecher method of a storage.PeerStorage by
// attempting to graduate one of 1000 nonexistent Peers.
//
// GradNonexist1k can run in parallel.
func (bh *benchHolder) GradNonexist1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_ = ps.GraduateLeecher(context.TODO(), bd.infoHashes[0], bd.peers[i%peersCount])
		return nil
	})
}

// GradNonexist1kInfoHash benchmarks the GraduateLeecher method of a storage.PeerStorage
// by attempting to graduate a nonexistent Peer for one of 100 InfoHashes.
//
// GradNonexist1kInfoHash can run in parallel.
func (bh *benchHolder) GradNonexist1kInfoHash(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_ = ps.GraduateLeecher(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[0])
		return nil
	})
}

// GradNonexist1kInfoHash1k benchmarks the GraduateLeecher method of a storage.PeerStorage
// by attempting to graduate one of 1000 nonexistent Peers for one of 1000
// infoHashes.
//
// GradNonexist1kInfoHash1k can run in parallel.
func (bh *benchHolder) GradNonexist1kInfoHash1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_ = ps.GraduateLeecher(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[(i*3)%peersCount])
		return nil
	})
}

// PutGradDelete benchmarks the PutLeecher, GraduateLeecher and DeleteSeeder
// methods of a storage.PeerStorage by adding one leecher to a swarm, promoting it to a
// seeder and deleting the seeder.
//
// PutGradDelete can not run in parallel.
func (bh *benchHolder) PutGradDelete(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutLeecher(context.TODO(), bd.infoHashes[0], bd.peers[0])
		if err != nil {
			return err
		}
		err = ps.GraduateLeecher(context.TODO(), bd.infoHashes[0], bd.peers[0])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(context.TODO(), bd.infoHashes[0], bd.peers[0])
	})
}

// PutGradDelete1k behaves like PutGradDelete with one of 1000 Peers.
//
// PutGradDelete1k can not run in parallel.
func (bh *benchHolder) PutGradDelete1k(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutLeecher(context.TODO(), bd.infoHashes[0], bd.peers[i%peersCount])
		if err != nil {
			return err
		}
		err = ps.GraduateLeecher(context.TODO(), bd.infoHashes[0], bd.peers[i%peersCount])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(context.TODO(), bd.infoHashes[0], bd.peers[i%peersCount])
	})
}

// PutGradDelete1kInfoHash behaves like PutGradDelete with one of 1000
// infoHashes.
//
// PutGradDelete1kInfoHash can not run in parallel.
func (bh *benchHolder) PutGradDelete1kInfoHash(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutLeecher(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[0])
		if err != nil {
			return err
		}
		err = ps.GraduateLeecher(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[0])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[0])
	})
}

// PutGradDelete1kInfoHash1k behaves like PutGradDelete with one of 1000 Peers
// and one of 1000 infoHashes.
//
// PutGradDelete1kInfoHash can not run in parallel.
func (bh *benchHolder) PutGradDelete1kInfoHash1k(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.PeerStorage, bd *benchData) error {
		err := ps.PutLeecher(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[(i*3)%peersCount])
		if err != nil {
			return err
		}
		err = ps.GraduateLeecher(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[(i*3)%peersCount])
		if err != nil {
			return err
		}
		err = ps.DeleteSeeder(context.TODO(), bd.infoHashes[i%ihCount], bd.peers[(i*3)%peersCount])
		return err
	})
}

func putPeers(ps storage.PeerStorage, bd *benchData) error {
	l := len(bd.peers)
	for _, ih := range bd.infoHashes {
		for i, peer := range bd.peers {
			var err error
			if i < l/2 {
				err = ps.PutLeecher(context.TODO(), ih, peer)
			} else {
				err = ps.PutSeeder(context.TODO(), ih, peer)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// AnnounceLeecher benchmarks the AnnouncePeers method of a storage.PeerStorage for
// announcing a leecher.
// The swarm announced to has 500 seeders and 500 leechers.
//
// AnnounceLeecher can run in parallel.
func (bh *benchHolder) AnnounceLeecher(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_, err := ps.AnnouncePeers(context.TODO(), bd.infoHashes[0], false, 50, bd.peers[0].Addr().Is6())
		return err
	})
}

// AnnounceLeecher1kInfoHash behaves like AnnounceLeecher with one of 1000
// infoHashes.
//
// AnnounceLeecher1kInfoHash can run in parallel.
func (bh *benchHolder) AnnounceLeecher1kInfoHash(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_, err := ps.AnnouncePeers(context.TODO(), bd.infoHashes[i%ihCount], false, 50, bd.peers[0].Addr().Is6())
		return err
	})
}

// AnnounceSeeder behaves like AnnounceLeecher with a seeder instead of a
// leecher.
//
// AnnounceSeeder can run in parallel.
func (bh *benchHolder) AnnounceSeeder(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_, err := ps.AnnouncePeers(context.TODO(), bd.infoHashes[0], true, 50, bd.peers[0].Addr().Is6())
		return err
	})
}

// AnnounceSeeder1kInfoHash behaves like AnnounceSeeder with one of 1000
// infoHashes.
//
// AnnounceSeeder1kInfoHash can run in parallel.
func (bh *benchHolder) AnnounceSeeder1kInfoHash(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_, err := ps.AnnouncePeers(context.TODO(), bd.infoHashes[i%ihCount], true, 50, bd.peers[0].Addr().Is6())
		return err
	})
}

// ScrapeSwarm benchmarks the ScrapeSwarm method of a storage.PeerStorage.
// The swarm scraped has 500 seeders and 500 leechers.
//
// ScrapeSwarm can run in parallel.
func (bh *benchHolder) ScrapeSwarm(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_, _, _, _ = ps.ScrapeSwarm(context.TODO(), bd.infoHashes[0])
		return nil
	})
}

// ScrapeSwarm1kInfoHash behaves like ScrapeSwarm with one of 1000 infoHashes.
//
// ScrapeSwarm1kInfoHash can run in parallel.
func (bh *benchHolder) ScrapeSwarm1kInfoHash(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.PeerStorage, bd *benchData) error {
		_, _, _, _ = ps.ScrapeSwarm(context.TODO(), bd.infoHashes[i%ihCount])
		return nil
	})
}

// RunBenchmarks starts series of benchmarks
func RunBenchmarks(b *testing.B, newStorage benchStorageConstructor) {
	bh := benchHolder{st: newStorage}
	b.Run("BenchmarkNop", bh.Nop)
	b.Run("BenchmarkPut", bh.Put)
	b.Run("BenchmarkPut1k", bh.Put1k)
	b.Run("BenchmarkPut1kInfoHash", bh.Put1kInfoHash)
	b.Run("BenchmarkPut1kInfoHash1k", bh.Put1kInfoHash1k)
	b.Run("BenchmarkPutDelete", bh.PutDelete)
	b.Run("BenchmarkPutDelete1k", bh.PutDelete1k)
	b.Run("BenchmarkPutDelete1kInfoHash", bh.PutDelete1kInfoHash)
	b.Run("BenchmarkPutDelete1kInfoHash1k", bh.PutDelete1kInfoHash1k)
	b.Run("BenchmarkDeleteNonexist", bh.DeleteNonexist)
	b.Run("BenchmarkDeleteNonexist1k", bh.DeleteNonexist1k)
	b.Run("BenchmarkDeleteNonexist1kInfoHash", bh.DeleteNonexist1kInfoHash)
	b.Run("BenchmarkDeleteNonexist1kInfoHash1k", bh.DeleteNonexist1kInfoHash1k)
	b.Run("BenchmarkPutGradDelete", bh.PutGradDelete)
	b.Run("BenchmarkPutGradDelete1k", bh.PutGradDelete1k)
	b.Run("BenchmarkPutGradDelete1kInfoHash", bh.PutGradDelete1kInfoHash)
	b.Run("BenchmarkPutGradDelete1kInfoHash1k", bh.PutGradDelete1kInfoHash1k)
	b.Run("BenchmarkGradNonexist", bh.GradNonexist)
	b.Run("BenchmarkGradNonexist1k", bh.GradNonexist1k)
	b.Run("BenchmarkGradNonexist1kInfoHash", bh.GradNonexist1kInfoHash)
	b.Run("BenchmarkGradNonexist1kInfoHash1k", bh.GradNonexist1kInfoHash1k)
	b.Run("BenchmarkAnnounceLeecher", bh.AnnounceLeecher)
	b.Run("BenchmarkAnnounceLeecher1kInfoHash", bh.AnnounceLeecher1kInfoHash)
	b.Run("BenchmarkAnnounceSeeder", bh.AnnounceSeeder)
	b.Run("BenchmarkAnnounceSeeder1kInfoHash", bh.AnnounceSeeder1kInfoHash)
	b.Run("BenchmarkScrapeSwarm", bh.ScrapeSwarm)
	b.Run("BenchmarkScrapeSwarm1kInfoHash", bh.ScrapeSwarm1kInfoHash)
}
