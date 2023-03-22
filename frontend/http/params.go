package http

import (
	"github.com/rs/zerolog"
	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/str2bytes"
	"github.com/valyala/fasthttp"
)

// queryParams parses a URL Query and implements the Params interface with some
// additional helpers.
type queryParams struct {
	*fasthttp.Args
}

// GetString returns a string parsed from a query. Every key can be returned as a
// string because they are encoded in the URL as strings.
func (qp queryParams) GetString(key string) (string, bool) {
	v := qp.Peek(key)
	return str2bytes.BytesToString(v), v != nil
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
