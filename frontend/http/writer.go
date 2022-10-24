package http

import (
	"bytes"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/anacrolix/torrent/bencode"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/bytepool"
)

var respBufferPool = bytepool.NewBufferPool()

// WriteError communicates an error to a BitTorrent client over HTTP.
func WriteError(w http.ResponseWriter, err error) {
	message := "internal server error"
	var clientErr bittorrent.ClientError
	if errors.As(err, &clientErr) {
		message = clientErr.Error()
	} else {
		logger.Error().Err(err).Msg("http: internal error")
	}
	_, _ = w.Write([]byte("d14:failure reason" + strconv.Itoa(len(message)) + ":" + message + "e"))
}

// WriteAnnounceResponse communicates the results of an Announce to a
// BitTorrent client over HTTP.
func WriteAnnounceResponse(w http.ResponseWriter, resp *bittorrent.AnnounceResponse) error {
	bb := respBufferPool.Get()
	defer respBufferPool.Put(bb)

	if resp.Interval > 0 {
		resp.Interval /= time.Second
	}

	if resp.Interval > 0 {
		resp.MinInterval /= time.Second
	}

	bb.WriteString("d8:completei")
	bb.WriteString(strconv.FormatUint(uint64(resp.Complete), 10))
	bb.WriteString("e10:incompletei")
	bb.WriteString(strconv.FormatUint(uint64(resp.Incomplete), 10))
	bb.WriteString("e8:intervali")
	bb.WriteString(strconv.FormatUint(uint64(resp.Interval), 10))
	bb.WriteString("e12:min intervali")
	bb.WriteString(strconv.FormatUint(uint64(resp.MinInterval), 10))
	bb.WriteByte('e')

	// Add the peers to the dictionary in the compact format.
	if resp.Compact {
		// Add the IPv4 peers to the dictionary.
		bb.WriteString("5:peersl")
		for _, peer := range resp.IPv4Peers {
			compactAddress(bb, peer)
		}
		bb.WriteByte('e')

		// Add the IPv6 peers to the dictionary.
		bb.WriteString("6:peers6l")
		for _, peer := range resp.IPv6Peers {
			compactAddress(bb, peer)
		}
		bb.WriteByte('e')
	} else {
		// Add the peers to the dictionary.
		bb.WriteString("5:peersl")
		for _, peer := range resp.IPv4Peers {
			dictAddress(bb, peer)
		}
		for _, peer := range resp.IPv6Peers {
			dictAddress(bb, peer)
		}
		bb.WriteByte('e')
	}
	bb.WriteByte('e')

	_, err := bb.WriteTo(w)
	return err
}

func compactAddress(bb *bytes.Buffer, peer bittorrent.Peer) {
	addr, port := peer.Addr().AsSlice(), peer.Port()
	bb.WriteString(strconv.Itoa(len(addr) + 2))
	bb.WriteByte(':')
	bb.Write(addr)
	bb.WriteByte(byte(port >> 8))
	bb.WriteByte(byte(port))
	return
}

func dictAddress(bb *bytes.Buffer, peer bittorrent.Peer) {
	bb.WriteString("d2:ip")
	addr := peer.Addr().String()
	bb.WriteString(strconv.Itoa(len(addr)))
	bb.WriteByte(':')
	bb.WriteString(addr)
	bb.WriteString("7:peer id20:")
	bb.WriteString(peer.ID.RawString())
	bb.WriteString("4:porti")
	bb.WriteString(strconv.FormatUint(uint64(peer.Port()), 10))
	bb.WriteString("ee")
}

// WriteScrapeResponse communicates the results of a Scrape to a BitTorrent
// client over HTTP.
func WriteScrapeResponse(w http.ResponseWriter, resp *bittorrent.ScrapeResponse) error {
	filesDict := make(map[bittorrent.InfoHash]any, len(resp.Files))
	for _, scrape := range resp.Files {
		filesDict[scrape.InfoHash] = map[string]any{
			"complete":   scrape.Complete,
			"downloaded": scrape.Snatches,
			"incomplete": scrape.Incomplete,
		}
	}

	return bencode.NewEncoder(w).Encode(map[string]any{
		"files": filesDict,
	})
}
