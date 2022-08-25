// Package varinterval contains interval variation middleware
package varinterval

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/storage"
)

// Name is the name by which this middleware is registered with Conf.
const Name = "interval variation"

func init() {
	middleware.RegisterBuilder(Name, build)
}

func build(options conf.MapConfig, _ storage.PeerStorage) (h middleware.Hook, err error) {
	var cfg Config

	if err = options.Unmarshal(&cfg); err != nil {
		err = fmt.Errorf("middleware %s: %w", Name, err)
	} else {
		if err := checkConfig(cfg); err == nil {
			h = &hook{
				cfg: cfg,
			}
		}
	}
	return
}

var (
	// ErrInvalidModifyResponseProbability is returned for a config with an invalid
	// ModifyResponseProbability.
	ErrInvalidModifyResponseProbability = errors.New("invalid modify_response_probability")

	// ErrInvalidMaxIncreaseDelta is returned for a config with an invalid
	// MaxIncreaseDelta.
	ErrInvalidMaxIncreaseDelta = errors.New("invalid max_increase_delta")
)

// Config represents the configuration for the varinterval middleware.
type Config struct {
	// ModifyResponseProbability is the probability by which a response will
	// be modified.
	ModifyResponseProbability float32 `cfg:"modify_response_probability"`

	// MaxIncreaseDelta is the amount of seconds that will be added at most.
	MaxIncreaseDelta int `cfg:"max_increase_delta"`

	// ModifyMinInterval specifies whether min_interval should be increased
	// as well.
	ModifyMinInterval bool `cfg:"modify_min_interval"`
}

func checkConfig(cfg Config) error {
	if cfg.ModifyResponseProbability <= 0 || cfg.ModifyResponseProbability > 1 {
		return ErrInvalidModifyResponseProbability
	}

	if cfg.MaxIncreaseDelta <= 0 {
		return ErrInvalidMaxIncreaseDelta
	}

	return nil
}

type hook struct {
	cfg Config
	sync.Mutex
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, resp *bittorrent.AnnounceResponse) (context.Context, error) {
	// Generate a probability p < 1.0.
	p, s0, s1 := xoroshiro128p(deriveEntropyFromRequest(req))
	if float32(float64(p)/math.MaxUint64) < h.cfg.ModifyResponseProbability {
		// Generate the increase delta.
		v, _, _ := xoroshiro128p(s0, s1)
		add := time.Duration(v%uint64(h.cfg.MaxIncreaseDelta)+1) * time.Second

		resp.Interval += add

		if h.cfg.ModifyMinInterval {
			resp.MinInterval += add
		}
	}

	return ctx, nil
}

func (h *hook) HandleScrape(ctx context.Context, _ *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	// Scrapes are not altered.
	return ctx, nil
}

// deriveEntropyFromRequest generates 2*64 bits of pseudo random state from an
// bittorrent.AnnounceRequest.
//
// Calling deriveEntropyFromRequest multiple times yields the same values.
func deriveEntropyFromRequest(req *bittorrent.AnnounceRequest) (v0 uint64, v1 uint64) {
	if len(req.InfoHash) >= bittorrent.InfoHashV1Len {
		v0 = binary.BigEndian.Uint64([]byte(req.InfoHash[:8])) + binary.BigEndian.Uint64([]byte(req.InfoHash[8:16]))
	}
	v1 = binary.BigEndian.Uint64(req.ID[:8]) + binary.BigEndian.Uint64(req.ID[8:16])
	return
}

// xoroshiro128p calculates predictable pseudorandom number
// with XOR/rotate/shift/rotate 128+ algorithm.
// see https://prng.di.unimi.it/xoroshiro128plus.c
func xoroshiro128p(s0, s1 uint64) (result, ns0, ns1 uint64) {
	result = s0 + s1
	s1 ^= s0
	ns0 = ((s0 << 24) | (s0 >> 40)) ^ s1 ^ (s1 << 16) // rotl(s0, 24) ^ s1 ^ (s1 << 16)
	ns1 = (s1 << 37) | (s1 >> 27)                     // rotl(s1, 37)
	return
}
