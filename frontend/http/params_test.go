package http

import (
	"bytes"
	"net/url"
	"testing"

	"github.com/valyala/fasthttp"
)

var (
	testPeerID = "-TEST01-6wfG2wk6wWLc"

	ValidAnnounceArguments = []url.Values{
		{},
		{"peer_id": {testPeerID}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}},
		{"peer_id": {testPeerID}, "ip": {"192.168.0.1"}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}},
		{"peer_id": {testPeerID}, "ip": {"192.168.0.1"}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}, "numwant": {"28"}},
		{"peer_id": {testPeerID}, "ip": {"192.168.0.1"}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}, "event": {"stopped"}},
		{"peer_id": {testPeerID}, "ip": {"192.168.0.1"}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}, "event": {"started"}, "numwant": {"13"}},
		{"peer_id": {testPeerID}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}, "no_peer_id": {"1"}},
		{"peer_id": {testPeerID}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}, "compact": {"0"}, "no_peer_id": {"1"}},
		{"peer_id": {testPeerID}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}, "compact": {"0"}, "no_peer_id": {"1"}, "key": {"peerKey"}},
		{"peer_id": {testPeerID}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}, "compact": {"0"}, "no_peer_id": {"1"}, "key": {"peerKey"}, "trackerid": {"trackerId"}},
		{"peer_id": {"%3Ckey%3A+0x90%3E"}, "port": {"6881"}, "downloaded": {"1234"}, "left": {"4321"}, "compact": {"0"}, "no_peer_id": {"1"}, "key": {"peerKey"}, "trackerid": {"trackerId"}},
		{"peer_id": {"%3Ckey%3A+0x90%3E"}, "compact": {"1"}},
		{"peer_id": {""}, "compact": {""}},
	}

	// See https://github.com/chihaya/chihaya/issues/334.
	shouldNotPanicQueries = [][]byte{
		[]byte("/annnounce?info_hash=" + testPeerID + "&a"),
		[]byte("/annnounce?info_hash=" + testPeerID + "&=b?"),
	}
)

func mapArrayEqual(boxed map[string][]string, unboxed *queryParams) (bool, string) {
	if len(boxed) != unboxed.Len() {
		return false, ""
	}

	for mapKey, mapVal := range boxed {
		// Always expect box to hold only one element
		if len(mapVal) != 1 || mapVal[0] != string(unboxed.Peek(mapKey)) {
			return false, mapVal[0]
		}
	}

	return true, ""
}

// parseURLData parses a request URL or UDP URLData as defined in BEP41.
// It expects a concatenated string of the request's path and query parts as
// defined in RFC 3986. As both the udp: and http: scheme used by BitTorrent
// include an authority part the path part must always begin with a slash.
// An example of the expected URLData would be "/announce?port=1234&uploaded=0"
// or "/?auth=0x1337".
// HTTP servers should pass (*http.Request).RequestURI, UDP servers should
// pass the concatenated, unchanged URLData as defined in BEP41.
//
// Note that, in the case of a key occurring multiple times in the query, only
// the last value for that key is kept.
// The only exception to this rule is the key "info_hash" which will attempt to
// parse each value as an InfoHash and return an error if parsing fails. All
// InfoHashes are collected and can later be retrieved by calling the InfoHashes
// method.
//
// Also note that any error that is encountered during parsing is returned as a
// ClientError, as this method is expected to be used to parse client-provided
// data.
func parseURLData(urlData []byte) *queryParams {
	i := bytes.IndexByte(urlData, '?')
	if i >= 0 {
		urlData = urlData[i+1:]
	}
	q := &queryParams{new(fasthttp.Args)}
	q.ParseBytes(urlData)
	return q
}

func TestParseEmptyURLData(t *testing.T) {
	parsedQuery := parseURLData(nil)
	if parsedQuery == nil {
		t.Fatal("Parsed query must not be nil")
	}
}

func TestParseValidURLData(t *testing.T) {
	for parseIndex, parseVal := range ValidAnnounceArguments {
		parsedQueryObj := parseURLData([]byte("/announce?" + parseVal.Encode()))

		if eq, exp := mapArrayEqual(parseVal, parsedQueryObj); !eq {
			t.Fatalf("Incorrect parse at item %d.\n Expected=%v\n Received=%v\n", parseIndex, parseVal, exp)
		}
	}
}

func TestParseShouldNotPanicURLData(_ *testing.T) {
	for _, parseStr := range shouldNotPanicQueries {
		_ = parseURLData(parseStr)
	}
}

func BenchmarkParseQuery(b *testing.B) {
	announceStrings := make([][]byte, 0)
	for i := range ValidAnnounceArguments {
		announceStrings = append(announceStrings, []byte(ValidAnnounceArguments[i].Encode()))
	}
	b.ResetTimer()
	for bCount := 0; bCount < b.N; bCount++ {
		i := bCount % len(announceStrings)
		_ = parseURLData(announceStrings[i])
	}
}

func BenchmarkURLParseQuery(b *testing.B) {
	announceStrings := make([]string, 0)
	for i := range ValidAnnounceArguments {
		announceStrings = append(announceStrings, ValidAnnounceArguments[i].Encode())
	}
	b.ResetTimer()
	for bCount := 0; bCount < b.N; bCount++ {
		i := bCount % len(announceStrings)
		parsedQueryObj, err := url.ParseQuery(announceStrings[i])
		if err != nil {
			b.Error(err, i)
			b.Log(parsedQueryObj)
		}
	}
}
