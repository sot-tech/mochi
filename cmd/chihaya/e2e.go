//go:build e2e
//+build e2e

package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/anacrolix/torrent/tracker"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/pkg/log"
)

func init() {
	e2eCmd = &cobra.Command{
		Use:   "e2e",
		Short: "exec e2e tests",
		Long:  "Execute the Chihaya end-to-end test suite",
		RunE:  EndToEndRunCmdFunc,
	}

	e2eCmd.Flags().String("httpaddr", "http://127.0.0.1:6969/announce", "address of the HTTP tracker")
	e2eCmd.Flags().String("udpaddr", "udp://127.0.0.1:6969", "address of the UDP tracker")
	e2eCmd.Flags().Duration("delay", time.Second, "delay between announces")
}

// EndToEndRunCmdFunc implements a Cobra command that runs the end-to-end test
// suite for a Chihaya build.
func EndToEndRunCmdFunc(cmd *cobra.Command, args []string) error {
	delay, err := cmd.Flags().GetDuration("delay")
	if err != nil {
		return err
	}

	// Test the HTTP tracker
	httpAddr, err := cmd.Flags().GetString("httpaddr")
	if err != nil {
		return err
	}

	if len(httpAddr) != 0 {
		log.Info("testing HTTP...")
		err := test(httpAddr, delay)
		if err != nil {
			return err
		}
		log.Info("success")
	}

	// Test the UDP tracker.
	udpAddr, err := cmd.Flags().GetString("udpaddr")
	if err != nil {
		return err
	}

	if len(udpAddr) != 0 {
		log.Info("testing UDP...")
		err := test(udpAddr, delay)
		if err != nil {
			return err
		}
		log.Info("success")
	}

	return nil
}

func test(addr string, delay time.Duration) error {
	b := make([]byte, bittorrent.InfoHashV1Len)
	rand.Read(b)
	ih, _ := bittorrent.NewInfoHash(b)
	return testWithInfohash(ih, addr, delay)
}

func testWithInfohash(infoHash bittorrent.InfoHash, url string, delay time.Duration) error {
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
		UserAgent:  "chihaya-e2e",
	}.Do()
	if err != nil {
		return errors.Wrap(err, "announce failed")
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
		UserAgent:  "chihaya-e2e",
	}.Do()
	if err != nil {
		return errors.Wrap(err, "announce failed")
	}

	if len(resp.Peers) != 1 {
		return fmt.Errorf("expected 1 peers, got %d", len(resp.Peers))
	}

	if resp.Peers[0].Port != int(req.Port) {
		return fmt.Errorf("expected port %d, got %d ", req.Port, resp.Peers[0].Port)
	}

	return nil
}
