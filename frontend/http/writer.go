package http

import (
	"bytes"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

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
		compactAddresses(bb, resp.IPv4Peers, false)
		// Add the IPv6 peers to the dictionary.
		compactAddresses(bb, resp.IPv6Peers, true)
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

func compactAddresses(bb *bytes.Buffer, peers bittorrent.Peers, v6 bool) {
	l := len(peers)
	if l > 0 {
		key, al := "5:peers", net.IPv4len
		if v6 {
			key, al = "6:peers6", net.IPv6len
		}
		bb.WriteString(key)
		bb.WriteString(strconv.Itoa((al + 2) * l))
		bb.WriteByte(':')
		for _, peer := range peers {
			bb.Write(peer.Addr().AsSlice())
			port := peer.Port()
			bb.WriteByte(byte(port >> 8))
			bb.WriteByte(byte(port))
		}
	}
}

func dictAddress(bb *bytes.Buffer, peer bittorrent.Peer) {
	bb.WriteString("d2:ip")
	addr := peer.Addr().String()
	bb.WriteString(strconv.Itoa(len(addr)))
	bb.WriteByte(':')
	bb.WriteString(addr)
	bb.WriteString("7:peer id20:")
	bb.Write(peer.ID[:])
	bb.WriteString("4:porti")
	bb.WriteString(strconv.FormatUint(uint64(peer.Port()), 10))
	bb.WriteString("ee")
}

// WriteScrapeResponse communicates the results of a Scrape to a BitTorrent
// client over HTTP.
func WriteScrapeResponse(w http.ResponseWriter, resp *bittorrent.ScrapeResponse) error {
	bb := respBufferPool.Get()
	defer respBufferPool.Put(bb)
	bb.WriteString("d5:filesd")
	for _, scrape := range resp.Files {
		bb.WriteString(strconv.Itoa(len(scrape.InfoHash)))
		bb.WriteByte(':')
		bb.Write([]byte(scrape.InfoHash))
		bb.WriteString("d8:completei")
		bb.WriteString(strconv.FormatUint(uint64(scrape.Complete), 10))
		bb.WriteString("e10:downloadedi")
		bb.WriteString(strconv.FormatUint(uint64(scrape.Snatches), 10))
		bb.WriteString("e10:incompletei")
		bb.WriteString(strconv.FormatUint(uint64(scrape.Incomplete), 10))
		bb.WriteString("ee")
	}
	bb.WriteString("ee")
	_, err := bb.WriteTo(w)
	return err
}
