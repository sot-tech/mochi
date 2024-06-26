package http

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/bytepool"
)

var respBufferPool = bytepool.NewBufferPool()

func writeErrorResponse(w io.StringWriter, err error) {
	message := "mochi internal error"
	var clientErr bittorrent.ClientError
	if errors.As(err, &clientErr) {
		message = clientErr.Error()
	} else {
		logger.Error().Err(err).Msg("internal error")
	}
	_, _ = w.WriteString("d14:failure reason" + strconv.Itoa(len(message)) + ":" + message + "e")
}

func writeAnnounceResponse(w io.Writer, resp *bittorrent.AnnounceResponse, compact, includePeerID bool) {
	bb := respBufferPool.Get()
	defer respBufferPool.Put(bb)

	if resp.Interval > 0 {
		resp.Interval /= time.Second
	}
	if resp.Interval > 0 {
		resp.MinInterval /= time.Second
	}

	bb.WriteString("d8:completei")
	bb.Write(fasthttp.AppendUint(nil, int(resp.Complete)))
	bb.WriteString("e10:incompletei")
	bb.Write(fasthttp.AppendUint(nil, int(resp.Incomplete)))
	bb.WriteString("e8:intervali")
	bb.Write(fasthttp.AppendUint(nil, int(resp.Interval)))
	bb.WriteString("e12:min intervali")
	bb.Write(fasthttp.AppendUint(nil, int(resp.MinInterval)))
	bb.WriteByte('e')

	// Add the peers to the dictionary in the compact format.
	if compact {
		// Add the IPv4 peers to the dictionary.
		compactAddresses(bb, resp.IPv4Peers, false)
		// Add the IPv6 peers to the dictionary.
		compactAddresses(bb, resp.IPv6Peers, true)
	} else {
		// Add the peers to the dictionary.
		bb.WriteString("5:peersl")
		for _, peer := range resp.IPv4Peers {
			dictAddress(bb, peer, includePeerID)
		}
		for _, peer := range resp.IPv6Peers {
			dictAddress(bb, peer, includePeerID)
		}
		bb.WriteByte('e')
	}
	bb.WriteByte('e')

	_, _ = bb.WriteTo(w)
}

func compactAddresses(bb *bytes.Buffer, peers bittorrent.Peers, v6 bool) {
	l := len(peers)
	if l > 0 {
		key, al := "5:peers", net.IPv4len
		if v6 {
			key, al = "6:peers6", net.IPv6len
		}
		bb.WriteString(key)
		bb.Write(fasthttp.AppendUint(nil, (al+2)*l))
		bb.WriteByte(':')
		for _, peer := range peers {
			bb.Write(peer.Addr().AsSlice())
			port := peer.Port()
			bb.Write([]byte{byte(port >> 8), byte(port)})
		}
	}
}

func dictAddress(bb *bytes.Buffer, peer bittorrent.Peer, includePeerID bool) {
	bb.WriteString("d2:ip")
	addr := peer.Addr().String()
	bb.Write(fasthttp.AppendUint(nil, len(addr)))
	bb.WriteByte(':')
	bb.WriteString(addr)
	if includePeerID {
		bb.WriteString("7:peer id20:")
		bb.Write(peer.ID.Bytes())
	}
	bb.WriteString("4:porti")
	port := peer.Port()
	bb.Write([]byte{byte(port >> 8), byte(port), 'e', 'e'})
}

func writeScrapeResponse(w io.Writer, resp *bittorrent.ScrapeResponse) {
	bb := respBufferPool.Get()
	defer respBufferPool.Put(bb)
	bb.WriteString("d5:filesd")
	l := len(resp.Data)
	if l > 0 {
		if l > 1 {
			sort.Slice(resp.Data, func(i, j int) bool {
				return resp.Data[i].InfoHash < resp.Data[j].InfoHash
			})
		}
		for _, scrape := range resp.Data {
			bb.Write(fasthttp.AppendUint(nil, len(scrape.InfoHash)))
			bb.WriteByte(':')
			bb.Write([]byte(scrape.InfoHash))
			bb.WriteString("d8:completei")
			bb.Write(fasthttp.AppendUint(nil, int(scrape.Complete)))
			bb.WriteString("e10:downloadedi")
			bb.Write(fasthttp.AppendUint(nil, int(scrape.Snatches)))
			bb.WriteString("e10:incompletei")
			bb.Write(fasthttp.AppendUint(nil, int(scrape.Incomplete)))
			bb.Write([]byte{'e', 'e'})
		}
	}
	bb.Write([]byte{'e', 'e'})
	_, _ = bb.WriteTo(w)
}
