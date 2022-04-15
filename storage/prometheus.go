// Package storage contains prometheus specific globals, used by storages
package storage

import "github.com/prometheus/client_golang/prometheus"

func init() {
	// Register the metrics.
	prometheus.MustRegister(
		PromGCDurationMilliseconds,
		PromInfoHashesCount,
		PromSeedersCount,
		PromLeechersCount,
	)
}

var (
	// PromGCDurationMilliseconds is a histogram used by storage to record the
	// durations of execution time required for removing expired peers.
	PromGCDurationMilliseconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "mochi_storage_gc_duration_milliseconds",
		Help:    "The time it takes to perform storage garbage collection",
		Buckets: prometheus.ExponentialBuckets(9.375, 2, 10),
	})

	// PromInfoHashesCount is a gauge used to hold the current total amount of
	// unique swarms being tracked by a storage.
	PromInfoHashesCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mochi_storage_infohashes_count",
		Help: "The number of Infohashes tracked",
	})

	// PromSeedersCount is a gauge used to hold the current total amount of
	// unique seeders per swarm.
	PromSeedersCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mochi_storage_seeders_count",
		Help: "The number of seeders tracked",
	})

	// PromLeechersCount is a gauge used to hold the current total amount of
	// unique leechers per swarm.
	PromLeechersCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mochi_storage_leechers_count",
		Help: "The number of leechers tracked",
	})
)
