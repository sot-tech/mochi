package bittorrent

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	b        = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	expected = "0102030405060708090a0b0c0d0e0f1011121314"
)

func TestPeerID_String(t *testing.T) {
	pid, err := NewPeerID(b)
	require.Nil(t, err)
	s := pid.String()
	require.Equal(t, expected, s)
}

func TestInfoHash_String(t *testing.T) {
	ih, err := NewInfoHash(b)
	require.Nil(t, err)
	require.Equal(t, expected, ih.String())
}

func TestPeer_String(t *testing.T) {
	pid, err := NewPeerID(b)
	require.Nil(t, err)
	id, _ := NewPeerID(b)
	peerStringTestCases := []struct {
		input    Peer
		expected string
	}{
		{
			input: Peer{
				ID:       id,
				AddrPort: netip.MustParseAddrPort("10.11.12.1:1234"),
			},
			expected: fmt.Sprintf("%s@[10.11.12.1]:1234", expected),
		},
		{
			input: Peer{
				ID:       id,
				AddrPort: netip.MustParseAddrPort("[2001:db8::ff00:42:8329]:1234"),
			},
			expected: fmt.Sprintf("%s@[2001:db8::ff00:42:8329]:1234", expected),
		},
	}
	for _, c := range peerStringTestCases {
		c.input.ID = pid
		got := c.input.String()
		require.Equal(t, c.expected, got)
	}
}
