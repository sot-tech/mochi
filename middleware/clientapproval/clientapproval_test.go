package clientapproval

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
)

var cases = []struct {
	cfg      Config
	peerID   string
	approved bool
}{
	// Client ID is whitelisted
	{
		Config{
			ClientIDList: []string{"010203"},
		},
		"01020304050607080900",
		true,
	},
	// Client ID is not whitelisted
	{
		Config{
			ClientIDList: []string{"010203"},
		},
		"10203040506070809000",
		false,
	},
	// Client ID is not blacklisted
	{
		Config{
			ClientIDList: []string{"010203"},
			Invert:       true,
		},
		"00000000001234567890",
		true,
	},
	// Client ID is blacklisted
	{
		Config{
			ClientIDList: []string{"123456"},
			Invert:       true,
		},
		"12345678900000000000",
		false,
	},
}

func TestHandleAnnounce(t *testing.T) {
	for _, tt := range cases {
		t.Run(fmt.Sprintf("testing peerid %s", tt.peerID), func(t *testing.T) {
			c := conf.MapConfig{"client_id_list": tt.cfg.ClientIDList, "invert": tt.cfg.Invert}
			h, err := build(c, nil)
			require.Nil(t, err)

			ctx := context.Background()
			req := &bittorrent.AnnounceRequest{}
			resp := &bittorrent.AnnounceResponse{}

			peerid, err := bittorrent.NewPeerID([]byte(tt.peerID))
			require.Nil(t, err)
			req.ID = peerid

			nctx, err := h.HandleAnnounce(ctx, req, resp)
			require.Equal(t, ctx, nctx)
			if tt.approved {
				require.NotEqual(t, err, ErrClientUnapproved)
			} else {
				require.Equal(t, err, ErrClientUnapproved)
			}
		})
	}
}
