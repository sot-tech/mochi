package udp

import (
	"bytes"
	"net/url"
	"strings"

	"github.com/rs/zerolog"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/str2bytes"
)

// ErrInvalidQueryEscape is returned when a query string contains invalid
// escapes.
var ErrInvalidQueryEscape = bittorrent.ClientError("invalid query escape")

// queryParams parses a URL Query and implements the Params interface
type queryParams struct {
	params map[string]string
}

// parseQuery parses a request URL or UDP URLData as defined in BEP41.
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
//
// Also note that any error that is encountered during parsing is returned as a
// ClientError, as this method is expected to be used to parse client-provided
// data.
func parseQuery(query []byte) (q *queryParams, err error) {
	queryDelim := bytes.IndexRune(query, '?')
	if queryDelim != -1 {
		query = query[queryDelim+1:]
	}
	// This is basically url.ParseQuery, but with a map[string]string
	// instead of map[string][]string for the values.
	q = &queryParams{
		params: make(map[string]string),
	}

	for len(query) > 0 {
		key := query
		if i := bytes.IndexAny(key, "&;"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = nil
		}
		if len(key) == 0 {
			continue
		}
		var value []byte
		if i := bytes.IndexRune(key, '='); i >= 0 {
			key, value = key[:i], key[i+1:]
		}
		var k, v string
		k, err = url.QueryUnescape(str2bytes.BytesToString(key))
		if err != nil {
			// QueryUnescape returns an error like "invalid escape: '%x'".
			// But frontends record these errors to prometheus, which generates
			// a lot of time series.
			// We log it here for debugging instead.
			return nil, ErrInvalidQueryEscape
		}
		v, err = url.QueryUnescape(str2bytes.BytesToString(value))
		if err != nil {
			// QueryUnescape returns an error like "invalid escape: '%x'".
			// But frontends record these errors to prometheus, which generates
			// a lot of time series.
			// We log it here for debugging instead.
			return nil, ErrInvalidQueryEscape
		}

		q.params[strings.ToLower(k)] = v
	}

	return q, nil
}

// GetString returns a string parsed from a query. Every key can be returned as a
// string because they are encoded in the URL as strings.
func (qp queryParams) GetString(key string) (string, bool) {
	value, ok := qp.params[strings.ToLower(key)]
	return value, ok
}

// MarshalZerologObject writes fields into zerolog event
func (qp queryParams) MarshalZerologObject(e *zerolog.Event) {
	for k, v := range qp.params {
		e.Str(k, v)
	}
}
