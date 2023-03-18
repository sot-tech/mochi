package udp

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/frontend"
	"github.com/sot-tech/mochi/pkg/bytepool"
)

const (
	connectActionID uint32 = iota
	announceActionID
	scrapeActionID
	errorActionID
	// action == 4 is the "old" IPv6 action used by opentracker, with a packet
	// format specified at
	// https://web.archive.org/web/20170503181830/http://opentracker.blog.h3q.com/2007/12/28/the-ipv6-situation/
	announceV6ActionID
)

// Option-Types as described in BEP 41 and BEP 45.
const (
	optionEndOfOptions = 0x0
	optionNOP          = 0x1
	optionURLData      = 0x2
)

var (
	// initialConnectionID is the magic initial connection ID specified by BEP 15.
	initialConnectionID = []byte{0, 0, 0x04, 0x17, 0x27, 0x10, 0x19, 0x80}

	// eventIDs map values described in BEP 15 to Events.
	eventIDs = []bittorrent.Event{
		bittorrent.None,
		bittorrent.Completed,
		bittorrent.Started,
		bittorrent.Stopped,
	}

	errMalformedPacket   = bittorrent.ClientError("malformed packet")
	errUnknownAction     = bittorrent.ClientError("unknown action ID")
	errBadConnectionID   = bittorrent.ClientError("bad connection ID")
	errUnknownOptionType = bittorrent.ClientError("unknown option type")
	errInvalidInfoHash   = bittorrent.ClientError("invalid info hash")
	errInvalidPeerID     = bittorrent.ClientError("invalid info hash")

	reqRespBufferPool = bytepool.NewBufferPool()
)

// ParseAnnounce parses an AnnounceRequest from a UDP request.
//
// If v6Action is true, the announce is parsed the
// "old opentracker way":
// https://web.archive.org/web/20170503181830/http://opentracker.blog.h3q.com/2007/12/28/the-ipv6-situation/
func ParseAnnounce(r Request, v6Action bool, opts frontend.ParseOptions) (*bittorrent.AnnounceRequest, error) {
	var err error
	ipEnd := 84 + net.IPv4len
	if v6Action {
		ipEnd = 84 + net.IPv6len
	}

	if len(r.Packet) < ipEnd+10 {
		return nil, errMalformedPacket
	}

	request := new(bittorrent.AnnounceRequest)

	// XXX: pure V2 hashes will cause invalid parsing,
	// but BEP-52 says, that V2 hashes SHOULD be truncated
	// FIXME: make sure that we have a copy of InfoHash
	request.InfoHash, err = bittorrent.NewInfoHash(r.Packet[16:36])
	if err != nil {
		return nil, errInvalidInfoHash
	}

	request.ID, err = bittorrent.NewPeerID(r.Packet[36:56])
	if err != nil {
		return nil, errInvalidPeerID
	}

	request.Downloaded = binary.BigEndian.Uint64(r.Packet[56:64])
	request.Left = binary.BigEndian.Uint64(r.Packet[64:72])
	request.Uploaded = binary.BigEndian.Uint64(r.Packet[72:80])

	eventID := int(r.Packet[83])
	if eventID >= len(eventIDs) {
		return nil, bittorrent.ErrUnknownEvent
	}
	request.Event, request.EventProvided = eventIDs[eventID], true

	request.Add(bittorrent.RequestAddress{Addr: r.IP})
	if opts.AllowIPSpoofing {
		if spoofed, ok := netip.AddrFromSlice(r.Packet[84:ipEnd]); ok {
			request.Add(bittorrent.RequestAddress{Addr: spoofed, Provided: true})
		}
	}

	request.NumWant, request.NumWantProvided = binary.BigEndian.Uint32(r.Packet[ipEnd+4:ipEnd+8]), true
	request.Port = binary.BigEndian.Uint16(r.Packet[ipEnd+8 : ipEnd+10])
	request.Params, err = handleOptionalParameters(r.Packet[ipEnd+10:])
	if err != nil {
		return nil, err
	}

	if err = bittorrent.SanitizeAnnounce(request, opts.MaxNumWant, opts.DefaultNumWant, opts.FilterPrivateIPs); err != nil {
		request = nil
	}

	return request, err
}

// handleOptionalParameters parses the optional parameters as described in BEP
// 41 and updates an announce with the values parsed.
func handleOptionalParameters(packet []byte) (bittorrent.Params, error) {
	if len(packet) == 0 {
		return parseQuery(nil)
	}

	buf := reqRespBufferPool.Get()
	defer reqRespBufferPool.Put(buf)

	for i := 0; i < len(packet); {
		option := packet[i]
		switch option {
		case optionEndOfOptions:
			return parseQuery(buf.Bytes())
		case optionNOP:
			i++
		case optionURLData:
			if i+1 >= len(packet) {
				return nil, errMalformedPacket
			}

			length := int(packet[i+1])
			if i+2+length > len(packet) {
				return nil, errMalformedPacket
			}

			n, err := buf.Write(packet[i+2 : i+2+length])
			if err != nil {
				return nil, err
			}
			if n != length {
				return nil, fmt.Errorf("expected to write %d bytes, wrote %d", length, n)
			}

			i += 2 + length
		default:
			return nil, errUnknownOptionType
		}
	}

	return parseQuery(buf.Bytes())
}

// ParseScrape parses a ScrapeRequest from a UDP request.
func ParseScrape(r Request, opts frontend.ParseOptions) (*bittorrent.ScrapeRequest, error) {
	// If a scrape isn't at least 36 bytes long, it's malformed.
	if len(r.Packet) < 36 {
		return nil, errMalformedPacket
	}

	// Skip past the initial headers and check that the bytes left equal the
	// length of a valid list of infohashes.
	r.Packet = r.Packet[16:]
	// Only V1 and V2to1 (truncated) allowed
	if len(r.Packet)%bittorrent.InfoHashV1Len != 0 {
		return nil, errMalformedPacket
	}

	// Allocate a list of infohashes and append it to the list until we're out.
	var infoHashes []bittorrent.InfoHash
	var err error
	var request *bittorrent.ScrapeRequest
	for len(r.Packet) >= bittorrent.InfoHashV1Len {
		var ih bittorrent.InfoHash
		// FIXME: make sure that we have a copy of InfoHash
		if ih, err = bittorrent.NewInfoHash(r.Packet[:bittorrent.InfoHashV1Len]); err == nil {
			infoHashes = append(infoHashes, ih)
			r.Packet = r.Packet[bittorrent.InfoHashV1Len:]
		} else {
			break
		}
	}
	if err == nil {
		// Sanitize the request.
		request = &bittorrent.ScrapeRequest{
			InfoHashes:       infoHashes,
			RequestAddresses: bittorrent.RequestAddresses{bittorrent.RequestAddress{Addr: r.IP}},
		}

		err = bittorrent.SanitizeScrape(request, opts.MaxScrapeInfoHashes, opts.FilterPrivateIPs)
	}

	return request, err
}
