package frontend

import "github.com/sot-tech/mochi/pkg/log"

var logger = log.NewLogger("frontend configurator")

// ParseOptions is the configuration used to parse an Announce Request.
//
// If AllowIPSpoofing is true, IPs provided via params will be used.
type ParseOptions struct {
	AllowIPSpoofing     bool   `cfg:"allow_ip_spoofing"`
	FilterPrivateIPs    bool   `cfg:"filter_private_ips"`
	MaxNumWant          uint32 `cfg:"max_numwant"`
	DefaultNumWant      uint32 `cfg:"default_numwant"`
	MaxScrapeInfoHashes uint32 `cfg:"max_scrape_infohashes"`
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
func (op ParseOptions) Validate() ParseOptions {
	valid := op
	if op.MaxNumWant <= 0 {
		valid.MaxNumWant = defaultMaxNumWant
		logger.Warn().
			Str("name", "MaxNumWant").
			Uint32("provided", op.MaxNumWant).
			Uint32("default", valid.MaxNumWant).
			Msg("falling back to default configuration")
	}

	if op.DefaultNumWant <= 0 {
		valid.DefaultNumWant = defaultDefaultNumWant
		logger.Warn().
			Str("name", "DefaultNumWant").
			Uint32("provided", op.DefaultNumWant).
			Uint32("default", valid.DefaultNumWant).
			Msg("falling back to default configuration")
	}

	if op.MaxScrapeInfoHashes <= 0 {
		valid.MaxScrapeInfoHashes = defaultMaxScrapeInfoHashes
		logger.Warn().
			Str("name", "MaxScrapeInfoHashes").
			Uint32("provided", op.MaxScrapeInfoHashes).
			Uint32("default", valid.MaxScrapeInfoHashes).
			Msg("falling back to default configuration")
	}
	return valid
}

// Default parser config constants.
const (
	defaultMaxNumWant          = 100
	defaultDefaultNumWant      = 50
	defaultMaxScrapeInfoHashes = 50
)
