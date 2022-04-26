package frontend

import "github.com/sot-tech/mochi/pkg/log"

// ParseOptions is the configuration used to parse an Announce Request.
//
// If AllowIPSpoofing is true, IPs provided via params will be used.
type ParseOptions struct {
	AllowIPSpoofing     bool   `cfg:"allow_ip_spoofing"`
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
		log.Warn("falling back to default configuration", log.Fields{
			"name":     "MaxNumWant",
			"provided": op.MaxNumWant,
			"default":  valid.MaxNumWant,
		})
	}

	if op.DefaultNumWant <= 0 {
		valid.DefaultNumWant = defaultDefaultNumWant
		log.Warn("falling back to default configuration", log.Fields{
			"name":     "DefaultNumWant",
			"provided": op.DefaultNumWant,
			"default":  valid.DefaultNumWant,
		})
	}

	if op.MaxScrapeInfoHashes <= 0 {
		valid.MaxScrapeInfoHashes = defaultMaxScrapeInfoHashes
		log.Warn("falling back to default configuration", log.Fields{
			"name":     "MaxScrapeInfoHashes",
			"provided": op.MaxScrapeInfoHashes,
			"default":  valid.MaxScrapeInfoHashes,
		})
	}
	return valid
}

// Default parser config constants.
const (
	defaultMaxNumWant          = 100
	defaultDefaultNumWant      = 50
	defaultMaxScrapeInfoHashes = 50
)
