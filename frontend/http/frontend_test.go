package http

import (
	cr "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/log"
)

var (
	// nolint:gosec
	addr   = fmt.Sprintf("127.0.0.1:%d", rand.Int63n(10000)+16384)
	hashes = make([]string, 10)
	peers  = make([]string, 10)
)

func init() {
	_ = log.ConfigureLogger("", "error", false, false)
	for i := range hashes {
		var bb []byte
		// nolint:gosec
		if rand.Int()%2 == 0 {
			bb = make([]byte, bittorrent.InfoHashV1Len)
		} else {
			bb = make([]byte, bittorrent.InfoHashV2Len)
		}
		if _, err := cr.Read(bb); err != nil {
			panic(err)
		}
		hashes[i] = hex.EncodeToString(bb)
	}

	for i := range peers {
		bb := make([]byte, bittorrent.PeerIDLen)
		if _, err := cr.Read(bb); err != nil {
			panic(err)
		}
		peers[i] = string(bb)
	}
	_, err := NewFrontend(map[string]any{
		"addr":             addr,
		"enable_keepalive": true,
		"ping_routes":      []string{"ping"},
	}, &middleware.Logic{})
	if err != nil {
		panic(err)
	}
}

func runGet(u string, checkResponse bool) (err error) {
	var r *http.Response
	// nolint:gosec
	if r, err = http.Get(u); err == nil {
		if r.StatusCode < 400 {
			if checkResponse {
				var out []byte
				if out, err = io.ReadAll(r.Body); err == nil {
					sout := string(out)
					if strings.Contains(sout, "failure reason") {
						return errors.New(sout)
					}
				}
			} else {
				_ = r.Body.Close()
			}
		} else {
			return errors.New(r.Status)
		}
	}
	return err
}

func BenchmarkPing(b *testing.B) {
	u := url.URL{
		Scheme: "http",
		Host:   addr,
		Path:   "ping",
	}
	us := u.String()
	for i := 0; i < b.N; i++ {
		if err := runGet(us, false); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkAnnounce(b *testing.B) {
	for i := 0; i < b.N; i++ {
		u := url.URL{
			Scheme: "http",
			Host:   addr,
			Path:   DefaultAnnounceRoute,
			RawQuery: url.Values{
				"event":      []string{bittorrent.StartedStr},
				"compact":    []string{"1"},
				"left":       []string{"100"},
				"downloaded": []string{"0"},
				"uploaded":   []string{"0"},
				"numwant":    []string{"1"},
				"port":       []string{"12345"},
				// nolint:gosec
				"info_hash": []string{hashes[rand.Intn(len(hashes))]},
				// nolint:gosec
				"peer_id": []string{peers[rand.Intn(len(peers))]},
			}.Encode(),
		}
		if err := runGet(u.String(), true); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkScrape(b *testing.B) {
	for i := 0; i < b.N; i++ {
		u := url.URL{
			Scheme:   "http",
			Host:     addr,
			Path:     DefaultScrapeRoute,
			RawQuery: url.Values{"info_hash": hashes[:len(hashes)/2]}.Encode(),
		}
		if err := runGet(u.String(), true); err != nil {
			b.Error(err)
		}
	}
}
