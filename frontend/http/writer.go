package http

import (
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/anacrolix/torrent/bencode"

	"github.com/sot-tech/mochi/bittorrent"
)

// WriteError communicates an error to a BitTorrent client over HTTP.
func WriteError(w http.ResponseWriter, err error) {
	message := "internal server error"
	var clientErr bittorrent.ClientError
	if errors.As(err, &clientErr) {
		message = clientErr.Error()
	} else {
		logger.Error().Err(err).Msg("http: internal error")
	}

	if err = bencode.NewEncoder(w).Encode(map[string]any{
		"failure reason": message,
	}); err != nil {
		logger.Error().Err(err).Msg("unable to encode message")
	}
}

// WriteAnnounceResponse communicates the results of an Announce to a
// BitTorrent client over HTTP.
func WriteAnnounceResponse(w http.ResponseWriter, resp *bittorrent.AnnounceResponse) error {
	if resp.Interval > 0 {
		resp.Interval /= time.Second
	}

	if resp.Interval > 0 {
		resp.MinInterval /= time.Second
	}

	bdict := map[string]any{
		"complete":     resp.Complete,
		"incomplete":   resp.Incomplete,
		"interval":     resp.Interval,
		"min interval": resp.MinInterval,
	}

	// Add the peers to the dictionary in the compact format.
	if resp.Compact {
		// Add the IPv4 peers to the dictionary.
		compactAddresses := make([]byte, 0, (net.IPv4len+2)*len(resp.IPv4Peers))
		for _, peer := range resp.IPv4Peers {
			compactAddresses = append(compactAddresses, compactAddress(peer)...)
		}
		if len(compactAddresses) > 0 {
			bdict["peers"] = compactAddresses
		}

		// Add the IPv6 peers to the dictionary.
		compactAddresses = make([]byte, 0, (net.IPv6len+2)*len(resp.IPv6Peers)) // IP + port
		for _, peer := range resp.IPv6Peers {
			compactAddresses = append(compactAddresses, compactAddress(peer)...)
		}
		if len(compactAddresses) > 0 {
			bdict["peers6"] = compactAddresses
		}
	} else {
		// Add the peers to the dictionary.
		peers := make([]map[string]any, 0, len(resp.IPv4Peers)+len(resp.IPv6Peers)) // IP + port
		for _, peer := range resp.IPv4Peers {
			peers = append(peers, dict(peer))
		}
		for _, peer := range resp.IPv6Peers {
			peers = append(peers, dict(peer))
		}
		bdict["peers"] = peers
	}

	return bencode.NewEncoder(w).Encode(bdict)
}

// WriteScrapeResponse communicates the results of a Scrape to a BitTorrent
// client over HTTP.
func WriteScrapeResponse(w http.ResponseWriter, resp *bittorrent.ScrapeResponse) error {
	filesDict := make(map[string]any, len(resp.Files))
	for _, scrape := range resp.Files {
		filesDict[string(scrape.InfoHash[:])] = map[string]any{
			"complete":   scrape.Complete,
			"incomplete": scrape.Incomplete,
		}
	}

	return bencode.NewEncoder(w).Encode(map[string]any{
		"files": filesDict,
	})
}

func compactAddress(peer bittorrent.Peer) (buf []byte) {
	buf = append(buf, peer.Addr().AsSlice()...)
	port := peer.Port()
	buf = append(buf, byte(port>>8), byte(port))
	return
}

func dict(peer bittorrent.Peer) map[string]any {
	return map[string]any{
		"peer id": peer.ID.RawString(),
		"ip":      peer.Addr(),
		"port":    peer.Port(),
	}
}
