// Package main contains End-to-End MoChi check implementation.
// not used in production
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/anacrolix/torrent/tracker"

	"github.com/sot-tech/mochi/bittorrent"
)

func main() {
	httpAddress := flag.String("httpaddr", "http://127.0.0.1:6969/announce", "address of the HTTP tracker")
	udpAddress := flag.String("udpaddr", "udp://127.0.0.1:6969", "address of the UDP tracker")
	delay := flag.Duration("delay", time.Second, "delay between announces")
	flag.Parse()

	if len(*httpAddress) != 0 {
		log.Println("testing HTTP...")
		err := test(*httpAddress, *delay)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("success")
	}

	if len(*udpAddress) != 0 {
		log.Println("testing UDP...")
		err := test(*udpAddress, *delay)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("success")
	}
}

func test(addr string, delay time.Duration) error {
	b := make([]byte, bittorrent.InfoHashV1Len)
	rand.Read(b)
	ih, _ := bittorrent.NewInfoHash(b)
	return testWithInfoHash(ih, addr, delay)
}

func testWithInfoHash(infoHash bittorrent.InfoHash, url string, delay time.Duration) error {
	var ih [bittorrent.InfoHashV1Len]byte
	req := tracker.AnnounceRequest{
		InfoHash:   ih,
		PeerId:     [bittorrent.PeerIDLen]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		Downloaded: 50,
		Left:       100,
		Uploaded:   50,
		Event:      tracker.Started,
		IPAddress:  uint32(50<<24 | 10<<16 | 12<<8 | 1),
		NumWant:    50,
		Port:       10001,
	}

	resp, err := tracker.Announce{
		TrackerUrl: url,
		Request:    req,
		UserAgent:  "mochi-e2e",
	}.Do()
	if err != nil {
		return fmt.Errorf("announce failed: %w", err)
	}

	if len(resp.Peers) != 1 {
		return fmt.Errorf("expected one peer, got %d", len(resp.Peers))
	}

	time.Sleep(delay)

	copy(ih[:], infoHash)
	req = tracker.AnnounceRequest{
		InfoHash:   ih,
		PeerId:     [bittorrent.PeerIDLen]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21},
		Downloaded: 50,
		Left:       100,
		Uploaded:   50,
		Event:      tracker.Started,
		IPAddress:  uint32(50<<24 | 10<<16 | 12<<8 | 2),
		NumWant:    50,
		Port:       10002,
	}

	resp, err = tracker.Announce{
		TrackerUrl: url,
		Request:    req,
		UserAgent:  "mochi-e2e",
	}.Do()
	if err != nil {
		return fmt.Errorf("announce failed: %w", err)
	}

	if len(resp.Peers) != 1 {
		return fmt.Errorf("expected 1 peers, got %d", len(resp.Peers))
	}

	if resp.Peers[0].Port != int(req.Port) {
		return fmt.Errorf("expected port %d, got %d ", req.Port, resp.Peers[0].Port)
	}

	return nil
}
