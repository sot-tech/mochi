package bittorrent

import (
	"context"
	"github.com/rs/zerolog"
)

// Params is used to fetch (optional) request parameters from an Announce.
// For HTTP Announces this includes the request path and parsed query, for UDP
// Announces this is the extracted path and parsed query from optional URLData
// as specified in BEP41.
//
// See ParseURLData for specifics on parsing and limitations.
type Params interface {
	// String returns a string parsed from a query. Every key can be
	// returned as a string because they are encoded in the URL as strings.
	String(key string) (string, bool)

	zerolog.LogObjectMarshaler
}

type routeParamsKey struct{}

// RouteParamsKey is a key for the context of a request that
// contains the named parameters from the http router.
var RouteParamsKey = routeParamsKey{}

// RouteParam is a type that contains the values from the named parameters
// on the route.
type RouteParam struct {
	Key   string
	Value string
}

// RouteParams is a collection of RouteParam instances.
type RouteParams []RouteParam

// ByName returns the value of the first RouteParam that matches the given
// name. If no matching RouteParam is found, an empty string is returned.
// In the event that a "catch-all" parameter is provided on the route and
// no value is matched, an empty string is returned. For example: a route of
// "/announce/*param" matches on "/announce/". However, ByName("param") will
// return an empty string.
func (rp RouteParams) ByName(name string) string {
	for _, p := range rp {
		if p.Key == name {
			return p.Value
		}
	}
	return ""
}

// InjectRouteParamsToContext returns new context with specified RouteParams placed in
// RouteParamsKey key
func InjectRouteParamsToContext(ctx context.Context, rp RouteParams) context.Context {
	if rp == nil {
		rp = RouteParams{}
	}
	return context.WithValue(ctx, RouteParamsKey, rp)
}

// RemapRouteParamsToBgContext returns new context with context.Background parent
// and copied RouteParams from inCtx
func RemapRouteParamsToBgContext(inCtx context.Context) context.Context {
	rp, isOk := inCtx.Value(RouteParamsKey).(RouteParams)
	if !isOk {
		logger.Warn().Msg("unable to fetch route parameters, probably jammed context")
		rp = RouteParams{}
	}
	return context.WithValue(context.Background(), RouteParamsKey, rp)
}
