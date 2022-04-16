package http

import (
	"errors"
	"net/http"
	"net/netip"

	"github.com/sot-tech/mochi/bittorrent"
)

// ParseOptions is the configuration used to parse an Announce Request.
//
// If AllowIPSpoofing is true, IPs provided via BitTorrent params will be used.
// If RealIPHeader is not empty string, the value of the first HTTP Header with
// that name will be used.
type ParseOptions struct {
	AllowIPSpoofing     bool   `cfg:"allow_ip_spoofing"`
	RealIPHeader        string `cfg:"real_ip_header"`
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

var (
	errNoInfoHash                 = bittorrent.ClientError("no info hash supplied")
	errMultipleInfoHashes         = bittorrent.ClientError("multiple info hashes supplied")
	errInvalidPeerID              = bittorrent.ClientError("peer ID invalid or not provided")
	errInvalidParameterLeft       = bittorrent.ClientError("parameter 'left' invalid or not provided")
	errInvalidParameterDownloaded = bittorrent.ClientError("parameter 'downloaded' invalid or not provided")
	errInvalidParameterUploaded   = bittorrent.ClientError("parameter 'uploaded' invalid or not provided")
	errInvalidParameterNumWant    = bittorrent.ClientError("parameter 'num want' invalid or not provided")
)

// ParseAnnounce parses an bittorrent.AnnounceRequest from an http.Request.
func ParseAnnounce(r *http.Request, opts ParseOptions) (*bittorrent.AnnounceRequest, error) {
	qp, err := bittorrent.ParseURLData(r.RequestURI)
	if err != nil {
		return nil, err
	}

	request := &bittorrent.AnnounceRequest{Params: qp}

	// Attempt to parse the event from the request.
	var eventStr string
	eventStr, request.EventProvided = qp.String("event")
	if request.EventProvided {
		if request.Event, err = bittorrent.NewEvent(eventStr); err != nil {
			return nil, err
		}
	} else {
		request.Event = bittorrent.None
	}

	// Determine if the client expects a compact response.
	compactStr, _ := qp.String("compact")
	request.Compact = compactStr != "" && compactStr != "0"

	// Parse the infohash from the request.
	infoHashes := qp.InfoHashes()
	if len(infoHashes) < 1 {
		return nil, errNoInfoHash
	}
	if len(infoHashes) > 1 {
		return nil, errMultipleInfoHashes
	}
	request.InfoHash = infoHashes[0]

	// Parse the PeerID from the request.
	peerID, ok := qp.String("peer_id")
	if !ok {
		return nil, errInvalidPeerID
	}
	request.Peer.ID, err = bittorrent.NewPeerID([]byte(peerID))
	if err != nil {
		return nil, errInvalidPeerID
	}
	// Determine the number of remaining bytes for the client.
	request.Left, err = qp.Uint("left", 64)
	if err != nil {
		return nil, errInvalidParameterLeft
	}

	// Determine the number of bytes downloaded by the client.
	request.Downloaded, err = qp.Uint("downloaded", 64)
	if err != nil {
		return nil, errInvalidParameterDownloaded
	}

	// Determine the number of bytes shared by the client.
	request.Uploaded, err = qp.Uint("uploaded", 64)
	if err != nil {
		return nil, errInvalidParameterUploaded
	}

	// Determine the number of peers the client wants in the response.
	numwant, err := qp.Uint("numwant", 32)
	if err != nil && !errors.Is(err, bittorrent.ErrKeyNotFound) {
		return nil, errInvalidParameterNumWant
	}
	// If there were no errors, the user actually provided the numwant.
	request.NumWantProvided = err == nil
	request.NumWant = uint32(numwant)

	// Parse the port where the client is listening.
	port, err := qp.Uint("port", 16)
	if err != nil {
		return nil, bittorrent.ErrInvalidPort
	}

	// Parse the IP address where the client is listening.
	ip, spoofed, err := requestedIP(r, qp, opts)
	if err != nil {
		return nil, bittorrent.ErrInvalidIP
	}
	request.Peer.AddrPort = netip.AddrPortFrom(ip, uint16(port))
	request.IPProvided = spoofed

	if err = bittorrent.SanitizeAnnounce(request, opts.MaxNumWant, opts.DefaultNumWant); err != nil {
		request = nil
	}

	return request, err
}

// ParseScrape parses an bittorrent.ScrapeRequest from an http.Request.
func ParseScrape(r *http.Request, opts ParseOptions) (*bittorrent.ScrapeRequest, error) {
	qp, err := bittorrent.ParseURLData(r.RequestURI)
	if err != nil {
		return nil, err
	}

	infoHashes := qp.InfoHashes()
	if len(infoHashes) < 1 {
		return nil, errNoInfoHash
	}

	request := &bittorrent.ScrapeRequest{
		InfoHashes: infoHashes,
		Params:     qp,
	}

	if err := bittorrent.SanitizeScrape(request, opts.MaxScrapeInfoHashes); err != nil {
		return nil, err
	}

	return request, nil
}

// requestedIP determines the IP address for a BitTorrent client request.
func requestedIP(r *http.Request, p bittorrent.Params, opts ParseOptions) (netip.Addr, bool, error) {
	if opts.AllowIPSpoofing {
		if ipstr, ok := p.String("ip"); ok {
			addr, err := netip.ParseAddr(ipstr)
			return addr, true, err
		}

		if ipstr, ok := p.String("ipv4"); ok {
			addr, err := netip.ParseAddr(ipstr)
			return addr, true, err
		}

		if ipstr, ok := p.String("ipv6"); ok {
			addr, err := netip.ParseAddr(ipstr)
			return addr, true, err
		}
	}

	if ipstr := r.Header.Get(opts.RealIPHeader); ipstr != "" && opts.RealIPHeader != "" {
		addr, err := netip.ParseAddr(ipstr)
		return addr, false, err
	}

	addrPort, err := netip.ParseAddrPort(r.RemoteAddr)
	return addrPort.Addr().Unmap(), false, err
}
