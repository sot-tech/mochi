package bittorrent

import (
	"net/netip"

	"github.com/sot-tech/mochi/pkg/log"
)

var (
	// ErrInvalidIP indicates an invalid IP for an Announce.
	ErrInvalidIP = ClientError("invalid IP")

	// ErrInvalidPort indicates an invalid Port for an Announce.
	ErrInvalidPort = ClientError("invalid port")
)

// SanitizeAnnounce enforces a max and default NumWant and coerces the peer's
// IP address into the proper format.
func SanitizeAnnounce(r *AnnounceRequest, maxNumWant, defaultNumWant uint32) error {
	if r.Port() == 0 {
		return ErrInvalidPort
	}

	if !r.NumWantProvided {
		r.NumWant = defaultNumWant
	} else if r.NumWant > maxNumWant {
		r.NumWant = maxNumWant
	}

	r.AddrPort = netip.AddrPortFrom(r.Addr(), r.Port())
	if !r.Addr().IsValid() || r.Addr().IsUnspecified() {
		return ErrInvalidIP
	}

	log.Debug("sanitized announce", r, log.Fields{
		"maxNumWant":     maxNumWant,
		"defaultNumWant": defaultNumWant,
	})
	return nil
}

// SanitizeScrape enforces a max number of infohashes for a single scrape
// request.
func SanitizeScrape(r *ScrapeRequest, maxScrapeInfoHashes uint32) error {
	if len(r.InfoHashes) > int(maxScrapeInfoHashes) {
		r.InfoHashes = r.InfoHashes[:maxScrapeInfoHashes]
	}

	r.AddrPort = netip.AddrPortFrom(r.Addr(), r.Port())
	if !r.Addr().IsValid() || r.Addr().IsUnspecified() {
		return ErrInvalidIP
	}

	log.Debug("sanitized scrape", r, log.Fields{
		"maxScrapeInfoHashes": maxScrapeInfoHashes,
	})
	return nil
}
