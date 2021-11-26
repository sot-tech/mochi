package test

import (
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/storage"
	"math/rand"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
)

type benchData struct {
	infohashes [1000]bittorrent.InfoHash
	peers      [1000]bittorrent.Peer
}

func generateInfohashes() (a [1000]bittorrent.InfoHash) {
	for i := range a {
		b := make([]byte, bittorrent.InfoHashV1Len)
		rand.Read(b)
		a[i], _ = bittorrent.NewInfoHash(b)
	}

	return
}

func generatePeers() (a [1000]bittorrent.Peer) {
	r := rand.New(rand.NewSource(0))
	for i := range a {
		ip := make([]byte, 4)
		n, err := r.Read(ip)
		if err != nil || n != 4 {
			panic("unable to create random bytes")
		}
		id := [bittorrent.PeerIDLen]byte{}
		n, err = r.Read(id[:])
		if err != nil || n != bittorrent.InfoHashV1Len {
			panic("unable to create random bytes")
		}
		port := uint16(r.Uint32())
		a[i] = bittorrent.Peer{
			ID:   id,
			IP:   bittorrent.IP{IP: net.IP(ip), AddressFamily: bittorrent.IPv4},
			Port: port,
		}
	}

	return
}

type benchExecFunc func(int, storage.Storage, *benchData) error
type benchSetupFunc func(storage.Storage, *benchData) error
type benchStorageConstructor func() storage.Storage

type benchHolder struct {
	st benchStorageConstructor
}

func (bh *benchHolder) runBenchmark(b *testing.B, parallel bool, sf benchSetupFunc, ef benchExecFunc) {
	ps := bh.st()
	bd := &benchData{generateInfohashes(), generatePeers()}
	spacing := int32(1000 / runtime.NumCPU())
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

	errChan := ps.Stop()
	for err := range errChan {
		b.Fatal(err)
	}
}

// Nop executes a no-op for each iteration.
// It should produce the same results for each storage.Storage.
// This can be used to get an estimate of the impact of the benchmark harness
// on benchmark results and an estimate of the general performance of the system
// benchmarked on.
//
// Nop can run in parallel.
func (bh *benchHolder) Nop(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.Storage, bd *benchData) error {
		return nil
	})
}

// Put benchmarks the PutSeeder method of a storage.Storage by repeatedly Putting the
// same Peer for the same InfoHash.
//
// Put can run in parallel.
func (bh *benchHolder) Put(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		return ps.PutSeeder(bd.infohashes[0], bd.peers[0])
	})
}

// Put1k benchmarks the PutSeeder method of a storage.Storage by cycling through 1000
// Peers and Putting them into the swarm of one infohash.
//
// Put1k can run in parallel.
func (bh *benchHolder) Put1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		return ps.PutSeeder(bd.infohashes[0], bd.peers[i%1000])
	})
}

// Put1kInfohash benchmarks the PutSeeder method of a storage.Storage by cycling
// through 1000 infohashes and putting the same peer into their swarms.
//
// Put1kInfohash can run in parallel.
func (bh *benchHolder) Put1kInfohash(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		return ps.PutSeeder(bd.infohashes[i%1000], bd.peers[0])
	})
}

// Put1kInfohash1k benchmarks the PutSeeder method of a storage.Storage by cycling
// through 1000 infohashes and 1000 Peers and calling Put with them.
//
// Put1kInfohash1k can run in parallel.
func (bh *benchHolder) Put1kInfohash1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutSeeder(bd.infohashes[i%1000], bd.peers[(i*3)%1000])
		return err
	})
}

// PutDelete benchmarks the PutSeeder and DeleteSeeder methods of a storage.Storage by
// calling PutSeeder followed by DeleteSeeder for one Peer and one infohash.
//
// PutDelete can not run in parallel.
func (bh *benchHolder) PutDelete(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutSeeder(bd.infohashes[0], bd.peers[0])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(bd.infohashes[0], bd.peers[0])
	})
}

// PutDelete1k benchmarks the PutSeeder and DeleteSeeder methods in the same way
// PutDelete does, but with one from 1000 Peers per iteration.
//
// PutDelete1k can not run in parallel.
func (bh *benchHolder) PutDelete1k(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutSeeder(bd.infohashes[0], bd.peers[i%1000])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(bd.infohashes[0], bd.peers[i%1000])
	})
}

// PutDelete1kInfohash behaves like PutDelete1k with 1000 infohashes instead of
// 1000 Peers.
//
// PutDelete1kInfohash can not run in parallel.
func (bh *benchHolder) PutDelete1kInfohash(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutSeeder(bd.infohashes[i%1000], bd.peers[0])
		if err != nil {
		}
		return ps.DeleteSeeder(bd.infohashes[i%1000], bd.peers[0])
	})
}

// PutDelete1kInfohash1k behaves like PutDelete1k with 1000 infohashes in
// addition to 1000 Peers.
//
// PutDelete1kInfohash1k can not run in parallel.
func (bh *benchHolder) PutDelete1kInfohash1k(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutSeeder(bd.infohashes[i%1000], bd.peers[(i*3)%1000])
		if err != nil {
			return err
		}
		err = ps.DeleteSeeder(bd.infohashes[i%1000], bd.peers[(i*3)%1000])
		return err
	})
}

// DeleteNonexist benchmarks the DeleteSeeder method of a storage.Storage by
// attempting to delete a Peer that is nonexistent.
//
// DeleteNonexist can run in parallel.
func (bh *benchHolder) DeleteNonexist(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		_ = ps.DeleteSeeder(bd.infohashes[0], bd.peers[0])
		return nil
	})
}

// DeleteNonexist1k benchmarks the DeleteSeeder method of a storage.Storage by
// attempting to delete one of 1000 nonexistent Peers.
//
// DeleteNonexist can run in parallel.
func (bh *benchHolder) DeleteNonexist1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		_ = ps.DeleteSeeder(bd.infohashes[0], bd.peers[i%1000])
		return nil
	})
}

// DeleteNonexist1kInfohash benchmarks the DeleteSeeder method of a storage.Storage by
// attempting to delete one Peer from one of 1000 infohashes.
//
// DeleteNonexist1kInfohash can run in parallel.
func (bh *benchHolder) DeleteNonexist1kInfohash(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		_ = ps.DeleteSeeder(bd.infohashes[i%1000], bd.peers[0])
		return nil
	})
}

// DeleteNonexist1kInfohash1k benchmarks the Delete method of a storage.Storage by
// attempting to delete one of 1000 Peers from one of 1000 Infohashes.
//
// DeleteNonexist1kInfohash1k can run in parallel.
func (bh *benchHolder) DeleteNonexist1kInfohash1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		_ = ps.DeleteSeeder(bd.infohashes[i%1000], bd.peers[(i*3)%1000])
		return nil
	})
}

// GradNonexist benchmarks the GraduateLeecher method of a storage.Storage by
// attempting to graduate a nonexistent Peer.
//
// GradNonexist can run in parallel.
func (bh *benchHolder) GradNonexist(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		_ = ps.GraduateLeecher(bd.infohashes[0], bd.peers[0])
		return nil
	})
}

// GradNonexist1k benchmarks the GraduateLeecher method of a storage.Storage by
// attempting to graduate one of 1000 nonexistent Peers.
//
// GradNonexist1k can run in parallel.
func (bh *benchHolder) GradNonexist1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		_ = ps.GraduateLeecher(bd.infohashes[0], bd.peers[i%1000])
		return nil
	})
}

// GradNonexist1kInfohash benchmarks the GraduateLeecher method of a storage.Storage
// by attempting to graduate a nonexistent Peer for one of 100 Infohashes.
//
// GradNonexist1kInfohash can run in parallel.
func (bh *benchHolder) GradNonexist1kInfohash(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		_ = ps.GraduateLeecher(bd.infohashes[i%1000], bd.peers[0])
		return nil
	})
}

// GradNonexist1kInfohash1k benchmarks the GraduateLeecher method of a storage.Storage
// by attempting to graduate one of 1000 nonexistent Peers for one of 1000
// infohashes.
//
// GradNonexist1kInfohash1k can run in parallel.
func (bh *benchHolder) GradNonexist1kInfohash1k(b *testing.B) {
	bh.runBenchmark(b, true, nil, func(i int, ps storage.Storage, bd *benchData) error {
		_ = ps.GraduateLeecher(bd.infohashes[i%1000], bd.peers[(i*3)%1000])
		return nil
	})
}

// PutGradDelete benchmarks the PutLeecher, GraduateLeecher and DeleteSeeder
// methods of a storage.Storage by adding one leecher to a swarm, promoting it to a
// seeder and deleting the seeder.
//
// PutGradDelete can not run in parallel.
func (bh *benchHolder) PutGradDelete(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutLeecher(bd.infohashes[0], bd.peers[0])
		if err != nil {
			return err
		}
		err = ps.GraduateLeecher(bd.infohashes[0], bd.peers[0])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(bd.infohashes[0], bd.peers[0])
	})
}

// PutGradDelete1k behaves like PutGradDelete with one of 1000 Peers.
//
// PutGradDelete1k can not run in parallel.
func (bh *benchHolder) PutGradDelete1k(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutLeecher(bd.infohashes[0], bd.peers[i%1000])
		if err != nil {
			return err
		}
		err = ps.GraduateLeecher(bd.infohashes[0], bd.peers[i%1000])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(bd.infohashes[0], bd.peers[i%1000])
	})
}

// PutGradDelete1kInfohash behaves like PutGradDelete with one of 1000
// infohashes.
//
// PutGradDelete1kInfohash can not run in parallel.
func (bh *benchHolder) PutGradDelete1kInfohash(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutLeecher(bd.infohashes[i%1000], bd.peers[0])
		if err != nil {
			return err
		}
		err = ps.GraduateLeecher(bd.infohashes[i%1000], bd.peers[0])
		if err != nil {
			return err
		}
		return ps.DeleteSeeder(bd.infohashes[i%1000], bd.peers[0])
	})
}

// PutGradDelete1kInfohash1k behaves like PutGradDelete with one of 1000 Peers
// and one of 1000 infohashes.
//
// PutGradDelete1kInfohash can not run in parallel.
func (bh *benchHolder) PutGradDelete1kInfohash1k(b *testing.B) {
	bh.runBenchmark(b, false, nil, func(i int, ps storage.Storage, bd *benchData) error {
		err := ps.PutLeecher(bd.infohashes[i%1000], bd.peers[(i*3)%1000])
		if err != nil {
			return err
		}
		err = ps.GraduateLeecher(bd.infohashes[i%1000], bd.peers[(i*3)%1000])
		if err != nil {
			return err
		}
		err = ps.DeleteSeeder(bd.infohashes[i%1000], bd.peers[(i*3)%1000])
		return err
	})
}

func putPeers(ps storage.Storage, bd *benchData) error {
	for i := 0; i < 1000; i++ {
		for j := 0; j < 1000; j++ {
			var err error
			if j < 1000/2 {
				err = ps.PutLeecher(bd.infohashes[i], bd.peers[j])
			} else {
				err = ps.PutSeeder(bd.infohashes[i], bd.peers[j])
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// AnnounceLeecher benchmarks the AnnouncePeers method of a storage.Storage for
// announcing a leecher.
// The swarm announced to has 500 seeders and 500 leechers.
//
// AnnounceLeecher can run in parallel.
func (bh *benchHolder) AnnounceLeecher(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.Storage, bd *benchData) error {
		_, err := ps.AnnouncePeers(bd.infohashes[0], false, 50, bd.peers[0])
		return err
	})
}

// AnnounceLeecher1kInfohash behaves like AnnounceLeecher with one of 1000
// infohashes.
//
// AnnounceLeecher1kInfohash can run in parallel.
func (bh *benchHolder) AnnounceLeecher1kInfohash(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.Storage, bd *benchData) error {
		_, err := ps.AnnouncePeers(bd.infohashes[i%1000], false, 50, bd.peers[0])
		return err
	})
}

// AnnounceSeeder behaves like AnnounceLeecher with a seeder instead of a
// leecher.
//
// AnnounceSeeder can run in parallel.
func (bh *benchHolder) AnnounceSeeder(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.Storage, bd *benchData) error {
		_, err := ps.AnnouncePeers(bd.infohashes[0], true, 50, bd.peers[0])
		return err
	})
}

// AnnounceSeeder1kInfohash behaves like AnnounceSeeder with one of 1000
// infohashes.
//
// AnnounceSeeder1kInfohash can run in parallel.
func (bh *benchHolder) AnnounceSeeder1kInfohash(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.Storage, bd *benchData) error {
		_, err := ps.AnnouncePeers(bd.infohashes[i%1000], true, 50, bd.peers[0])
		return err
	})
}

// ScrapeSwarm benchmarks the ScrapeSwarm method of a storage.Storage.
// The swarm scraped has 500 seeders and 500 leechers.
//
// ScrapeSwarm can run in parallel.
func (bh *benchHolder) ScrapeSwarm(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.Storage, bd *benchData) error {
		ps.ScrapeSwarm(bd.infohashes[0], bittorrent.IPv4)
		return nil
	})
}

// ScrapeSwarm1kInfohash behaves like ScrapeSwarm with one of 1000 infohashes.
//
// ScrapeSwarm1kInfohash can run in parallel.
func (bh *benchHolder) ScrapeSwarm1kInfohash(b *testing.B) {
	bh.runBenchmark(b, true, putPeers, func(i int, ps storage.Storage, bd *benchData) error {
		ps.ScrapeSwarm(bd.infohashes[i%1000], bittorrent.IPv4)
		return nil
	})
}

func RunBenchmarks(b *testing.B, newStorage benchStorageConstructor) {
	bh := benchHolder{st: newStorage}
	b.Run("BenchmarkNop", bh.Nop)
	b.Run("BenchmarkPut", bh.Put)
	b.Run("BenchmarkPut1k", bh.Put1k)
	b.Run("BenchmarkPut1kInfohash", bh.Put1kInfohash)
	b.Run("BenchmarkPut1kInfohash1k", bh.Put1kInfohash1k)
	b.Run("BenchmarkPutDelete", bh.PutDelete)
	b.Run("BenchmarkPutDelete1k", bh.PutDelete1k)
	b.Run("BenchmarkPutDelete1kInfohash", bh.PutDelete1kInfohash)
	b.Run("BenchmarkPutDelete1kInfohash1k", bh.PutDelete1kInfohash1k)
	b.Run("BenchmarkDeleteNonexist", bh.DeleteNonexist)
	b.Run("BenchmarkDeleteNonexist1k", bh.DeleteNonexist1k)
	b.Run("BenchmarkDeleteNonexist1kInfohash", bh.DeleteNonexist1kInfohash)
	b.Run("BenchmarkDeleteNonexist1kInfohash1k", bh.DeleteNonexist1kInfohash1k)
	b.Run("BenchmarkPutGradDelete", bh.PutGradDelete)
	b.Run("BenchmarkPutGradDelete1k", bh.PutGradDelete1k)
	b.Run("BenchmarkPutGradDelete1kInfohash", bh.PutGradDelete1kInfohash)
	b.Run("BenchmarkPutGradDelete1kInfohash1k", bh.PutGradDelete1kInfohash1k)
	b.Run("BenchmarkGradNonexist", bh.GradNonexist)
	b.Run("BenchmarkGradNonexist1k", bh.GradNonexist1k)
	b.Run("BenchmarkGradNonexist1kInfohash", bh.GradNonexist1kInfohash)
	b.Run("BenchmarkGradNonexist1kInfohash1k", bh.GradNonexist1kInfohash1k)
	b.Run("BenchmarkAnnounceLeecher", bh.AnnounceLeecher)
	b.Run("BenchmarkAnnounceLeecher1kInfohash", bh.AnnounceLeecher1kInfohash)
	b.Run("BenchmarkAnnounceSeeder", bh.AnnounceSeeder)
	b.Run("BenchmarkAnnounceSeeder1kInfohash", bh.AnnounceSeeder1kInfohash)
	b.Run("BenchmarkScrapeSwarm", bh.ScrapeSwarm)
	b.Run("BenchmarkScrapeSwarm1kInfohash", bh.ScrapeSwarm1kInfohash)
}
