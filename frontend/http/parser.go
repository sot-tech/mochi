package http

import (
	"errors"
	"net/http"
	"net/netip"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/frontend"
)

// ParseOptions is the configuration used to parse an Announce Request.
//
// If AllowIPSpoofing is true, IPs provided via BitTorrent params will be used.
// If RealIPHeader is not empty string, the value of the first HTTP Header with
// that name will be used.
type ParseOptions struct {
	frontend.ParseOptions
	RealIPHeader string `cfg:"real_ip_header"`
}

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

	// Parse the info hash from the request.
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
	request.ID, err = bittorrent.NewPeerID([]byte(peerID))
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
	numWant, err := qp.Uint("numwant", 32)
	if err != nil && !errors.Is(err, bittorrent.ErrKeyNotFound) {
		return nil, errInvalidParameterNumWant
	}
	// If there were no errors, the user actually provided the numWant.
	request.NumWantProvided = err == nil
	request.NumWant = uint32(numWant)

	// Parse the port where the client is listening.
	port, err := qp.Uint("port", 16)
	if err != nil {
		return nil, bittorrent.ErrInvalidPort
	}
	request.Port = uint16(port)

	// Parse the IP address where the client is listening.
	request.RequestAddresses = requestedIPs(r, qp, opts)

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
		InfoHashes:       infoHashes,
		Params:           qp,
		RequestAddresses: requestedIPs(r, qp, opts),
	}

	err = bittorrent.SanitizeScrape(request, opts.MaxScrapeInfoHashes)

	return request, err
}

// requestedIPs determines the IP address for a BitTorrent client request.
func requestedIPs(r *http.Request, p bittorrent.Params, opts ParseOptions) (addresses bittorrent.RequestAddresses) {
	if opts.AllowIPSpoofing {
		for _, f := range []string{"ip", "ipv4", "ipv6"} {
			if ipStr, ok := p.String(f); ok {
				addresses.Add(parseRequestAddress(ipStr, true))
			}
		}
	}

	if ipStr := r.Header.Get(opts.RealIPHeader); ipStr != "" && opts.RealIPHeader != "" {
		addresses.Add(parseRequestAddress(ipStr, false))
	} else {
		addrPort, _ := netip.ParseAddrPort(r.RemoteAddr)
		addresses.Add(bittorrent.RequestAddress{
			Addr:     addrPort.Addr(),
			Provided: false,
		})
	}
	return
}

func parseRequestAddress(s string, provided bool) (ra bittorrent.RequestAddress) {
	a, e := netip.ParseAddr(s)
	if e == nil {
		ra.Addr, ra.Provided = a, provided
	}
	return
}
