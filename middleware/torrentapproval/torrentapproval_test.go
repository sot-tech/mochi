package torrentapproval

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage/memory"
)

func init() {
	_ = log.ConfigureLogger("", "warn", false, false)
}

var cases = []struct {
	cfg      baseConfig
	ih       string
	approved bool
}{
	// Infohash is whitelisted
	{
		baseConfig{
			Source: "list",
			Configuration: map[string]any{
				"hash_list": []string{"3532cf2d327fad8448c075b4cb42c8136964a435"},
			},
		},
		"3532cf2d327fad8448c075b4cb42c8136964a435",
		true,
	},
	// Infohash is not whitelisted
	{
		baseConfig{
			Source: "list",
			Configuration: map[string]any{
				"hash_list": []string{"3532cf2d327fad8448c075b4cb42c8136964a435"},
			},
		},
		"4532cf2d327fad8448c075b4cb42c8136964a435",
		false,
	},
	// Infohash is not blacklisted
	{
		baseConfig{
			Source: "list",
			Configuration: map[string]any{
				"hash_list": []string{"3532cf2d327fad8448c075b4cb42c8136964a435"},
				"invert":    true,
			},
		},
		"4532cf2d327fad8448c075b4cb42c8136964a435",
		true,
	},
	// Infohash is blacklisted
	{
		baseConfig{
			Source: "list",
			Configuration: map[string]any{
				"hash_list": []string{"3532cf2d327fad8448c075b4cb42c8136964a435"},
				"invert":    true,
			},
		},
		"3532cf2d327fad8448c075b4cb42c8136964a435",
		false,
	},
}

func TestHandleAnnounce(t *testing.T) {
	config := memory.Config{}.Validate()
	storage, err := memory.NewPeerStorage(config)
	require.Nil(t, err)
	for _, tt := range cases {
		t.Run(fmt.Sprintf("testing hash %s", tt.ih), func(t *testing.T) {
			cfg := conf.MapConfig{"initial_source": tt.cfg.Source, "configuration": tt.cfg.Configuration}
			h, err := build(cfg, storage)
			require.Nil(t, err)

			ctx := context.Background()
			req := &bittorrent.AnnounceRequest{}
			resp := &bittorrent.AnnounceResponse{}

			hashinfo, err := bittorrent.NewInfoHash(tt.ih)
			require.Nil(t, err)

			req.InfoHash = hashinfo

			nctx, err := h.HandleAnnounce(ctx, req, resp)
			require.Equal(t, ctx, nctx)
			if tt.approved == true {
				require.NotEqual(t, err, ErrTorrentUnapproved)
			} else {
				require.Equal(t, err, ErrTorrentUnapproved)
			}
		})
	}
}
