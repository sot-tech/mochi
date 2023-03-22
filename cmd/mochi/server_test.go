package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cr "crypto/rand"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/frontend"
	hf "github.com/sot-tech/mochi/frontend/http"
	l "github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/str2bytes"
)

const (
	connectUDPPacketSize  = 16
	announceUDPPacketSize = 98
	announceNumWant       = 10

	timeout = time.Second * 2
)

var (
	udpConnectHeader       = []byte{0x0, 0x0, 0x4, 0x17, 0x27, 0x10, 0x19, 0x80}
	errUDPSendTruncated    = errors.New("data not fully sent")
	errUDPRecvTruncated    = errors.New("data not fully received")
	errUDPUnexpectedAction = errors.New("unexpected action")
	errTxIDMissmatch       = errors.New("transaction ID missmatch")
	hashes                 = make([][]byte, 100)
	peers                  = make([][]byte, 100)
)

func init() {
	_ = l.ConfigureLogger("", "error", false, false)
	for i := range hashes {
		bb := make([]byte, bittorrent.InfoHashV1Len)
		if _, err := cr.Read(bb); err != nil {
			panic(err)
		}
		hashes[i] = bb
	}

	for i := range peers {
		bb := make([]byte, bittorrent.PeerIDLen)
		if _, err := cr.Read(bb); err != nil {
			panic(err)
		}
		peers[i] = bb
	}
}

func buildUDPConnReq() []byte {
	req := make([]byte, connectUDPPacketSize)
	copy(req, udpConnectHeader)

	// TxID
	binary.BigEndian.PutUint32(req[12:16], rand.Uint32())
	return req
}

func sendUDPConnReq(addr string) ([]byte, []byte, error) {
	req := buildUDPConnReq()

	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))

	defer conn.Close()

	n, err := conn.Write(req)
	if err != nil {
		return nil, nil, err
	}

	if n != len(req) {
		return nil, nil, errUDPSendTruncated
	}

	resp := make([]byte, 16)
	n, err = conn.Read(resp)
	if err != nil {
		return nil, nil, err
	}

	if n != len(resp) {
		return nil, nil, errUDPRecvTruncated
	}

	action := binary.BigEndian.Uint32(resp[:4])
	if action != 0 {
		return nil, nil, errUDPUnexpectedAction
	}

	if !bytes.Equal(resp[4:8], req[12:16]) {
		return nil, nil, errTxIDMissmatch
	}

	// TxID, ConnectionID
	return resp[4:8], resp[8:16], nil
}

func buildAnnounceUDPReq(txID, connID []byte) []byte {
	req := make([]byte, announceUDPPacketSize)

	// Connection ID
	copy(req[:8], connID)

	// Action
	req[11] = 1

	// TxID
	copy(req[12:16], txID)

	// InfoHash
	copy(req[16:36], hashes[rand.Intn(len(hashes))])

	// PeerID
	copy(req[36:56], peers[rand.Intn(len(peers))])

	var down, left uint64
	if rand.Intn(2) == 0 {
		down, left = 1, 0
	} else {
		down, left = 0, 1
	}
	// Downloaded
	binary.BigEndian.PutUint64(req[56:64], down)
	// Left
	binary.BigEndian.PutUint64(req[64:72], left)

	// Event
	req[83] = 1

	// Numwant
	req[92], req[95] = byte(announceNumWant>>24), byte(announceNumWant>>16)

	// Port
	p := rand.Intn(math.MaxInt16) + 1
	req[96], req[97] = byte(p>>8), byte(p)
	return req
}

func BenchmarkServerUDPAnnounce(b *testing.B) {
	var s Server
	if err := s.Run(QuickConfig); err != nil {
		b.Fatal(err)
	}
	defer s.Shutdown()

	addr := "127.0.0.1" + frontend.DefaultListenAddress

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txID, connID, err := sendUDPConnReq(addr)
			if err != nil {
				b.Log(err)
				return
			}
			req := buildAnnounceUDPReq(txID, connID)

			conn, err := net.Dial("udp", addr)
			if err != nil {
				b.Log(err)
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(timeout))
			n, err := conn.Write(req)
			if err != nil {
				_ = conn.Close()
				b.Log(err)
				return
			}
			if n != announceUDPPacketSize {
				_ = conn.Close()
				b.Logf("packet not fully sent, %d bytes instead of %d", n, announceUDPPacketSize)
				return
			}
			resp := make([]byte, 1024)
			n, err = conn.Read(resp)
			_ = conn.Close()
			if err != nil {
				b.Log(err)
				return
			}
			if n < 20 {
				b.Logf("packet not fully received, only %d bytes", n)
				return
			}
			action := binary.BigEndian.Uint32(resp[:4])
			if action != 1 {
				if action == 3 {
					errVal := string(resp[8:n])
					b.Logf("tracker error: %s", errVal)
				} else {
					b.Logf("unexpected action: %d", action)
				}
				return
			}

			if !bytes.Equal(resp[4:8], req[12:16]) {
				b.Log("transaction ID missmatch")
			}
		}
	})
}

func sendHTTPReq(u string) (err error) {
	var r *http.Response
	// nolint:gosec
	if r, err = http.Get(u); err == nil {
		defer r.Body.Close()
		if r.StatusCode < 400 {
			var out []byte
			if out, err = io.ReadAll(r.Body); err == nil {
				sout := string(out)
				if strings.Contains(sout, "failure reason") {
					return errors.New(sout)
				}
			}
		} else {
			return errors.New(r.Status)
		}
	}
	return
}

func BenchmarkServerHTTPAnnounce(b *testing.B) {
	var s Server
	if err := s.Run(QuickConfig); err != nil {
		b.Fatal(err)
	}
	defer s.Shutdown()

	reqs := make([]string, len(hashes)*len(peers))
	addr := "127.0.0.1" + frontend.DefaultListenAddress
	for i := range reqs {
		var down, left string
		if rand.Intn(2) == 0 {
			down, left = "1", "0"
		} else {
			down, left = "0", "1"
		}
		u := url.URL{
			Scheme: "http",
			Host:   addr,
			Path:   hf.DefaultAnnounceRoute,
			RawQuery: url.Values{
				"event":      []string{bittorrent.StartedStr},
				"compact":    []string{"1"},
				"left":       []string{left},
				"downloaded": []string{down},
				"uploaded":   []string{"0"},
				"numwant":    []string{"1"},
				"port":       []string{strconv.FormatInt(int64(rand.Intn(math.MaxInt16)+1), 10)},
				"info_hash":  []string{str2bytes.BytesToString(hashes[rand.Intn(len(hashes))])},
				"peer_id":    []string{str2bytes.BytesToString(peers[rand.Intn(len(peers))])},
			}.Encode(),
		}
		reqs[i] = u.String()
	}

	var cnt atomic.Uint32
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := sendHTTPReq(reqs[int(cnt.Add(1))%len(reqs)]); err != nil {
				b.Log(err)
			}
		}
	})
}
