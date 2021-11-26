package torrentapproval

import (
	"context"
	"fmt"
	"github.com/chihaya/chihaya/middleware"
	"github.com/chihaya/chihaya/storage/memory"
	"gopkg.in/yaml.v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chihaya/chihaya/bittorrent"
)

var cases = []struct {
	cfg      middleware.Config
	ih       string
	approved bool
}{
	// Infohash is whitelisted
	{
		middleware.Config{
			Name: "list",
			Options: map[string]interface{}{
				"hash_list": []string{"3532cf2d327fad8448c075b4cb42c8136964a435"},
			},
		},
		"3532cf2d327fad8448c075b4cb42c8136964a435",
		true,
	},
	// Infohash is not whitelisted
	{
		middleware.Config{
			Name: "list",
			Options: map[string]interface{}{
				"hash_list": []string{"3532cf2d327fad8448c075b4cb42c8136964a435"},
			},
		},
		"4532cf2d327fad8448c075b4cb42c8136964a435",
		false,
	},
	// Infohash is not blacklisted
	{
		middleware.Config{
			Name: "list",
			Options: map[string]interface{}{
				"hash_list": []string{"3532cf2d327fad8448c075b4cb42c8136964a435"},
				"invert":    true,
			},
		},
		"4532cf2d327fad8448c075b4cb42c8136964a435",
		true,
	},
	// Infohash is blacklisted
	{
		middleware.Config{
			Name: "list",
			Options: map[string]interface{}{
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
	storage, err := memory.New(config)
	require.Nil(t, err)
	for _, tt := range cases {
		t.Run(fmt.Sprintf("testing hash %s", tt.ih), func(t *testing.T) {
			d := driver{}
			cfg, err := yaml.Marshal(tt.cfg)
			require.Nil(t, err)
			h, err := d.NewHook(cfg, storage)
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
