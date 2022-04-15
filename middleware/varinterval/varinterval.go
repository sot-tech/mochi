// Package varinterval contains interval variation middleware
package varinterval

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/middleware/pkg/random"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/storage"
)

// Name is the name by which this middleware is registered with Conf.
const Name = "interval variation"

func init() {
	middleware.RegisterBuilder(Name, build)
}

func build(options conf.MapConfig, _ storage.Storage) (h middleware.Hook, err error) {
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
	s0, s1 := random.DeriveEntropyFromRequest(req)
	// Generate a probability p < 1.0.
	v, s0, s1 := random.Intn(s0, s1, 1<<24)
	p := float32(v) / (1 << 24)
	if h.cfg.ModifyResponseProbability == 1 || p < h.cfg.ModifyResponseProbability {
		// Generate the increase delta.
		v, _, _ = random.Intn(s0, s1, h.cfg.MaxIncreaseDelta)
		add := time.Duration(v+1) * time.Second

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
