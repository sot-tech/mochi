package http

import (
	"bytes"
	"github.com/rs/zerolog"
	"github.com/sot-tech/mochi/bittorrent"
	"github.com/valyala/fasthttp"
)

// queryParams parses a URL Query and implements the Params interface with some
// additional helpers.
type queryParams struct {
	*fasthttp.Args
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
func parseURLData(urlData []byte) (*queryParams, error) {
	i := bytes.IndexByte(urlData, '?')
	if i >= 0 {
		urlData = urlData[i+1:]
	}
	q := &queryParams{new(fasthttp.Args)}
	q.ParseBytes(urlData)
	return q, nil
}

// String returns a string parsed from a query. Every key can be returned as a
// string because they are encoded in the URL as strings.
func (qp queryParams) String(key string) (string, bool) {
	v := qp.Peek(key)
	return string(v), v != nil
}

// InfoHashes returns a list of requested infohashes.
func (qp queryParams) InfoHashes() bittorrent.InfoHashes {
	var ihs bittorrent.InfoHashes
	for _, bb := range qp.PeekMulti("info_hash") {
		if ih, err := bittorrent.NewInfoHash(bb); err == nil {
			ihs = append(ihs, ih)
		}
	}
	return ihs
}

// MarshalZerologObject writes fields into zerolog event
func (qp queryParams) MarshalZerologObject(e *zerolog.Event) {
	e.Stringer("query", qp.Args)
}
