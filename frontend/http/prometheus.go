package http

import (
	"errors"
	"net/netip"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/metrics"
)

func init() {
	prometheus.MustRegister(promResponseDurationMilliseconds)
}

var promResponseDurationMilliseconds = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "mochi_http_response_duration_milliseconds",
		Help:    "The duration of time it takes to receive and write a response to an API request",
		Buckets: prometheus.ExponentialBuckets(9.375, 2, 10),
	},
	[]string{"action", "address_family", "error"},
)

// recordResponseDuration records the duration of time to respond to a Request
// in milliseconds.
func recordResponseDuration(action string, addr netip.Addr, err error, duration time.Duration) {
	var errString string
	if err != nil {
		var clientErr bittorrent.ClientError
		if errors.As(err, &clientErr) {
			errString = clientErr.Error()
		} else {
			errString = "internal error"
		}
	}

	promResponseDurationMilliseconds.
		WithLabelValues(action, metrics.AddressFamily(addr), errString).
		Observe(float64(duration.Nanoseconds()) / float64(time.Millisecond))
}
