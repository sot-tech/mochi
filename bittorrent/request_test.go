package bittorrent

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRequestAddresses_Validate test fix issue RM#5617
func TestRequestAddresses_Validate(t *testing.T) {
	ra := make(RequestAddresses, 0, 3)
	ra = append(ra, RequestAddress{
		Addr:     netip.MustParseAddr("1.2.3.4"),
		Provided: false,
	})
	ra = append(ra, RequestAddress{
		Addr:     netip.MustParseAddr("1.2.3.4"),
		Provided: true,
	})
	ra = append(ra, RequestAddress{
		Addr:     netip.MustParseAddr("4.3.2.1"),
		Provided: false,
	})
	ra = append(ra, RequestAddress{
		Addr:     netip.MustParseAddr("4.3.2.1"),
		Provided: false,
	})
	require.True(t, ra.Validate())
	require.Equal(t, 2, len(ra))
}
