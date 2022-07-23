package bittorrent

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

var addresses = RequestAddresses{
	RequestAddress{
		Addr:     netip.MustParseAddr("1.2.3.4"), // valid global
		Provided: false,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("1.2.3.4"), // valid global (duplicated)
		Provided: true,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("4.3.2.1"), // valid global
		Provided: false,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("4.3.2.1"), // valid global (duplicated)
		Provided: false,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("10.0.0.1"), // valid local
		Provided: false,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("172.16.0.1"), // valid local
		Provided: true,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("192.168.0.1"), // valid local
		Provided: false,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("127.0.0.1"), // valid loopback
		Provided: true,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("224.0.0.1"), // invalid (multicast)
		Provided: true,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("233.252.0.1"), // invalid (multicast)
		Provided: true,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("255.255.255.255"), // invalid (broadcast)
		Provided: true,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("169.254.0.1"), // valid link-local
		Provided: true,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("ff01::1"), // invalid (multicast)
		Provided: true,
	}, RequestAddress{
		Addr:     netip.MustParseAddr("fe80::1"), // valid link-local
		Provided: true,
	},
}

// TestRequestAddresses_SanitizeWPrivate test fix issue RM#5617
func TestRequestAddresses_SanitizeWPrivate(t *testing.T) {
	ra := make(RequestAddresses, len(addresses))
	copy(ra, addresses)
	require.True(t, ra.Sanitize(false))
	require.Equal(t, 8, len(ra))
}

// TestRequestAddresses_SanitizeAll test fix issue RM#5783
func TestRequestAddresses_SanitizeWOPrivate(t *testing.T) {
	ra := make(RequestAddresses, len(addresses))
	copy(ra, addresses)
	require.True(t, ra.Sanitize(true))
	require.Equal(t, 2, len(ra))
}
