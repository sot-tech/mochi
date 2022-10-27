package bittorrent

import (
	"github.com/sot-tech/mochi/pkg/log"
)

var (
	logger = log.NewLogger("bittorrent/sanitize")
	// ErrInvalidIP indicates an invalid IP for an Announce.
	ErrInvalidIP = ClientError("invalid IP")

	// ErrInvalidPort indicates an invalid Port for an Announce.
	ErrInvalidPort = ClientError("invalid port")
)

// SanitizeAnnounce enforces a max and default NumWant and coerces the peer's
// IP address into the proper format.
func SanitizeAnnounce(r *AnnounceRequest, maxNumWant, defaultNumWant uint32, filterPrivate bool) error {
	logger.Trace().Object("request", r).Msg("source announce")
	if r.Port == 0 {
		return ErrInvalidPort
	}

	if !r.Sanitize(filterPrivate) {
		return ErrInvalidIP
	}

	if !r.NumWantProvided {
		r.NumWant = defaultNumWant
	} else if r.NumWant > maxNumWant {
		r.NumWant = maxNumWant
	}

	logger.Trace().Object("request", r).Msg("sanitized announce")
	return nil
}

// SanitizeScrape enforces a max number of infohashes for a single scrape
// request and checks if addresses are valid.
func SanitizeScrape(r *ScrapeRequest, maxScrapeInfoHashes uint32, filterPrivate bool) error {
	logger.Trace().Object("request", r).Msg("source scrape")
	if len(r.InfoHashes) > int(maxScrapeInfoHashes) {
		r.InfoHashes = r.InfoHashes[:maxScrapeInfoHashes]
	}

	if !r.Sanitize(filterPrivate) {
		return ErrInvalidIP
	}

	logger.Trace().Object("request", r).Msg("sanitized scrape")
	return nil
}
