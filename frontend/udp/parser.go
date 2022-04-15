package udp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"sync"

	"github.com/sot-tech/mochi/bittorrent"
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
)

// ParseOptions is the configuration used to parse an Announce Request.
//
// If AllowIPSpoofing is true, IPs provided via params will be used.
type ParseOptions struct {
	AllowIPSpoofing     bool   `cfg:"allow_ip_spoofing"`
	MaxNumWant          uint32 `cfg:"max_numwant"`
	DefaultNumWant      uint32 `cfg:"default_numwant"`
	MaxScrapeInfoHashes uint32 `cfg:"max_scrape_infohashes"`
}

// Default parser config constants.
const (
	defaultMaxNumWant          = 100
	defaultDefaultNumWant      = 50
	defaultMaxScrapeInfoHashes = 50
)

// ParseAnnounce parses an AnnounceRequest from a UDP request.
//
// If v6Action is true, the announce is parsed the
// "old opentracker way":
// https://web.archive.org/web/20170503181830/http://opentracker.blog.h3q.com/2007/12/28/the-ipv6-situation/
func ParseAnnounce(r Request, v6Action bool, opts ParseOptions) (*bittorrent.AnnounceRequest, error) {
	ipEnd := 84 + net.IPv4len
	if v6Action {
		ipEnd = 84 + net.IPv6len
	}

	if len(r.Packet) < ipEnd+10 {
		return nil, errMalformedPacket
	}

	// XXX: pure V2 hashes will cause invalid parsing
	infohash := r.Packet[16:36]
	peerIDBytes := r.Packet[36:56]
	downloaded := binary.BigEndian.Uint64(r.Packet[56:64])
	left := binary.BigEndian.Uint64(r.Packet[64:72])
	uploaded := binary.BigEndian.Uint64(r.Packet[72:80])

	eventID := int(r.Packet[83])
	if eventID >= len(eventIDs) {
		return nil, bittorrent.ErrUnknownEvent
	}

	ip := r.IP
	ipProvided := false
	if opts.AllowIPSpoofing {
		ipBytes := r.Packet[84:ipEnd]
		spoofed, ok := netip.AddrFromSlice(ipBytes)
		if !ok {
			return nil, bittorrent.ErrInvalidIP
		}
		ipProvided = true
		ip = spoofed
	}
	if !opts.AllowIPSpoofing && r.IP.IsUnspecified() {
		// We have no IP address to fallback on.
		return nil, bittorrent.ErrInvalidIP
	}

	numWant := binary.BigEndian.Uint32(r.Packet[ipEnd+4 : ipEnd+8])
	port := binary.BigEndian.Uint16(r.Packet[ipEnd+8 : ipEnd+10])

	params, err := handleOptionalParameters(r.Packet[ipEnd+10:])
	if err != nil {
		return nil, err
	}

	ih, err := bittorrent.NewInfoHash(infohash)
	if err != nil {
		return nil, err
	}

	peerID, err := bittorrent.NewPeerID(peerIDBytes)
	if err != nil {
		return nil, err
	}

	request := &bittorrent.AnnounceRequest{
		Event:           eventIDs[eventID],
		InfoHash:        ih,
		NumWant:         numWant,
		Left:            left,
		Downloaded:      downloaded,
		Uploaded:        uploaded,
		IPProvided:      ipProvided,
		NumWantProvided: true,
		EventProvided:   true,
		Peer: bittorrent.Peer{
			ID:       peerID,
			AddrPort: netip.AddrPortFrom(ip, port),
		},
		Params: params,
	}

	if err = bittorrent.SanitizeAnnounce(request, opts.MaxNumWant, opts.DefaultNumWant); err != nil {
		request = nil
	}

	return request, err
}

type buffer struct {
	bytes.Buffer
}

var bufferFree = sync.Pool{
	New: func() any { return new(buffer) },
}

func newBuffer() *buffer {
	return bufferFree.Get().(*buffer)
}

func (b *buffer) free() {
	b.Reset()
	bufferFree.Put(b)
}

// handleOptionalParameters parses the optional parameters as described in BEP
// 41 and updates an announce with the values parsed.
func handleOptionalParameters(packet []byte) (bittorrent.Params, error) {
	if len(packet) == 0 {
		return bittorrent.ParseURLData("")
	}

	buf := newBuffer()
	defer buf.free()

	for i := 0; i < len(packet); {
		option := packet[i]
		switch option {
		case optionEndOfOptions:
			return bittorrent.ParseURLData(buf.String())
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

	return bittorrent.ParseURLData(buf.String())
}

// ParseScrape parses a ScrapeRequest from a UDP request.
func ParseScrape(r Request, opts ParseOptions) (*bittorrent.ScrapeRequest, error) {
	// If a scrape isn't at least 36 bytes long, it's malformed.
	if len(r.Packet) < 36 {
		return nil, errMalformedPacket
	}

	// Skip past the initial headers and check that the bytes left equal the
	// length of a valid list of infohashes.
	r.Packet = r.Packet[16:]
	l := len(r.Packet)
	isV1, isV2 := l%bittorrent.InfoHashV1Len == 0, l%bittorrent.InfoHashV2Len == 0

	if !(isV1 || isV2) {
		return nil, errMalformedPacket
	}

	// Allocate a list of infohashes and append it to the list until we're out.
	var infohashes []bittorrent.InfoHash
	var err error
	var request *bittorrent.ScrapeRequest
	pageSize := bittorrent.InfoHashV1Len
	if isV2 {
		pageSize = bittorrent.InfoHashV2Len
	}
	for len(r.Packet) >= pageSize {
		var ih bittorrent.InfoHash
		if ih, err = bittorrent.NewInfoHash(r.Packet[:pageSize]); err == nil {
			infohashes = append(infohashes, ih)
			r.Packet = r.Packet[pageSize:]
		} else {
			break
		}
	}
	if err == nil {
		// Sanitize the request.
		request = &bittorrent.ScrapeRequest{InfoHashes: infohashes}
		err = bittorrent.SanitizeScrape(request, opts.MaxScrapeInfoHashes)
	}

	return request, err
}
