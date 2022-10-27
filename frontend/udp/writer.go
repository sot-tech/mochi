package udp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/sot-tech/mochi/bittorrent"
)

// WriteError writes the failure reason as a null-terminated string.
func WriteError(w io.Writer, txID []byte, err error) {
	// If the client wasn't at fault, acknowledge it.
	var clientErr bittorrent.ClientError
	if !errors.Is(err, &clientErr) {
		err = fmt.Errorf("internal error occurred: %w", err)
	}

	buf := reqRespBufferPool.Get()
	defer reqRespBufferPool.Put(buf)
	writeHeader(buf, txID, errorActionID)
	_, _ = buf.WriteString(err.Error())
	_, _ = buf.WriteRune('\000')
	_, _ = w.Write(buf.Bytes())
}

// WriteAnnounce encodes an announce response according to BEP 15.
// The peers returned will be resp.IPv6Peers or resp.IPv4Peers, depending on
// whether v6Peers is set.
// If v6Action is set, the action will be 4, according to
// https://web.archive.org/web/20170503181830/http://opentracker.blog.h3q.com/2007/12/28/the-ipv6-situation/
func WriteAnnounce(w io.Writer, txID []byte, resp *bittorrent.AnnounceResponse, v6Action, v6Peers bool) {
	buf := reqRespBufferPool.Get()
	defer reqRespBufferPool.Put(buf)

	if v6Action {
		writeHeader(buf, txID, announceV6ActionID)
	} else {
		writeHeader(buf, txID, announceActionID)
	}
	_ = binary.Write(buf, binary.BigEndian, uint32(resp.Interval/time.Second))
	_ = binary.Write(buf, binary.BigEndian, resp.Incomplete)
	_ = binary.Write(buf, binary.BigEndian, resp.Complete)

	peers := resp.IPv4Peers
	if v6Peers {
		peers = resp.IPv6Peers
	}

	for _, peer := range peers {
		_, _ = buf.Write(peer.Addr().AsSlice())
		_ = binary.Write(buf, binary.BigEndian, peer.Port())
	}

	_, _ = w.Write(buf.Bytes())
}

// WriteScrape encodes a scrape response according to BEP 15.
func WriteScrape(w io.Writer, txID []byte, resp *bittorrent.ScrapeResponse) {
	buf := reqRespBufferPool.Get()
	defer reqRespBufferPool.Put(buf)

	writeHeader(buf, txID, scrapeActionID)

	for _, scrape := range resp.Files {
		_ = binary.Write(buf, binary.BigEndian, scrape.Complete)
		_ = binary.Write(buf, binary.BigEndian, scrape.Snatches)
		_ = binary.Write(buf, binary.BigEndian, scrape.Incomplete)
	}

	_, _ = w.Write(buf.Bytes())
}

// WriteConnectionID encodes a new connection response according to BEP 15.
func WriteConnectionID(w io.Writer, txID, connID []byte) {
	buf := reqRespBufferPool.Get()
	defer reqRespBufferPool.Put(buf)

	writeHeader(buf, txID, connectActionID)
	_, _ = buf.Write(connID)

	_, _ = w.Write(buf.Bytes())
}

// writeHeader writes the action and transaction ID to the provided response
// buffer.
func writeHeader(w io.Writer, txID []byte, action uint32) {
	_ = binary.Write(w, binary.BigEndian, action)
	_, _ = w.Write(txID)
}
