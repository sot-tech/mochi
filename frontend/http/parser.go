package http

import (
	"bytes"
	"errors"
	"net/netip"

	"github.com/valyala/fasthttp"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/frontend"
	"github.com/sot-tech/mochi/pkg/str2bytes"
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

// parseAnnounce parses an bittorrent.AnnounceRequest from an http.Request.
func parseAnnounce(r *fasthttp.RequestCtx, opts ParseOptions) (*bittorrent.AnnounceRequest, error) {
	qp := &queryParams{r.QueryArgs()}

	request := &bittorrent.AnnounceRequest{Params: qp}

	// Attempt to parse the event from the request.
	var eventStr string
	var err error
	eventStr, request.EventProvided = qp.GetString("event")
	if request.EventProvided {
		if request.Event, err = bittorrent.NewEvent(eventStr); err != nil {
			return nil, err
		}
	} else {
		request.Event = bittorrent.None
	}

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
	request.ID, err = bittorrent.NewPeerID(qp.Peek("peer_id"))
	if err != nil {
		return nil, errInvalidPeerID
	}
	// Determine the number of remaining bytes for the client.
	n, err := qp.GetUint("left")
	if err != nil {
		return nil, errInvalidParameterLeft
	}
	request.Left = uint64(n)

	// Determine the number of bytes downloaded by the client.
	n, err = qp.GetUint("downloaded")
	if err != nil {
		return nil, errInvalidParameterDownloaded
	}
	request.Downloaded = uint64(n)

	// Determine the number of bytes shared by the client.
	n, err = qp.GetUint("uploaded")
	if err != nil {
		return nil, errInvalidParameterUploaded
	}
	request.Uploaded = uint64(n)

	// Determine the number of peers the client wants in the response.
	n, err = qp.GetUint("numwant")
	if err != nil && !errors.Is(err, fasthttp.ErrNoArgValue) {
		return nil, errInvalidParameterNumWant
	}
	// If there were no errors, the user actually provided the numWant.
	request.NumWantProvided = err == nil
	request.NumWant = uint32(n)

	// Parse the port where the client is listening.
	n, err = qp.GetUint("port")
	if err != nil {
		return nil, bittorrent.ErrInvalidPort
	}
	request.Port = uint16(n)

	// Parse the IP address where the client is listening.
	request.RequestAddresses = requestedIPs(r, qp, opts)

	if err = bittorrent.SanitizeAnnounce(request, opts.MaxNumWant, opts.DefaultNumWant, opts.FilterPrivateIPs); err != nil {
		request = nil
	}

	return request, err
}

// parseScrape parses an bittorrent.ScrapeRequest from an http.Request.
func parseScrape(r *fasthttp.RequestCtx, opts ParseOptions) (*bittorrent.ScrapeRequest, error) {
	qp := &queryParams{r.QueryArgs()}

	infoHashes := qp.InfoHashes()
	if len(infoHashes) < 1 {
		return nil, errNoInfoHash
	}

	request := &bittorrent.ScrapeRequest{
		// FIXME: make sure that we have a copy of InfoHashes
		InfoHashes:       infoHashes,
		Params:           qp,
		RequestAddresses: requestedIPs(r, qp, opts),
	}

	err := bittorrent.SanitizeScrape(request, opts.MaxScrapeInfoHashes, opts.FilterPrivateIPs)

	return request, err
}

// requestedIPs determines the IP address for a BitTorrent client request.
func requestedIPs(r *fasthttp.RequestCtx, p *queryParams, opts ParseOptions) (addresses bittorrent.RequestAddresses) {
	if opts.AllowIPSpoofing {
		for _, f := range []string{"ip", "ipv4", "ipv6"} {
			if ipStr, ok := p.GetString(f); ok {
				addresses.Add(parseRequestAddress(ipStr, true))
			}
		}
	}

	if ipValues := r.Request.Header.PeekAll(opts.RealIPHeader); len(ipValues) > 0 && opts.RealIPHeader != "" {
		for _, ipStr := range ipValues {
			for _, ipStr := range bytes.Split(ipStr, []byte{','}) {
				if ipStr = bytes.TrimSpace(ipStr); len(ipStr) > 0 {
					addresses.Add(parseRequestAddress(str2bytes.BytesToString(ipStr), false))
				}
			}
		}
	} else {
		addrPort, _ := netip.ParseAddrPort(r.RemoteAddr().String())
		addresses.Add(bittorrent.RequestAddress{
			Addr:     addrPort.Addr(),
			Provided: false,
		})
	}
	return
}

func parseRequestAddress(s string, provided bool) (ra bittorrent.RequestAddress) {
	if addr, err := netip.ParseAddr(s); err == nil {
		ra.Addr, ra.Provided = addr, provided
	}
	return
}
