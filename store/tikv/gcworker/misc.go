package gcworker

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	gcWorkerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_worker_actions_total",
			Help:      "Counter of gc worker actions.",
		}, []string{"type"})

	gcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_seconds",
			Help:      "Bucketed histogram of gc duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		}, []string{"stage"})

	gcConfigGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_config",
			Help:      "Gauge of GC configs.",
		}, []string{"type"},
	)
)

func init() {
	prometheus.MustRegister(gcWorkerCounter)
	prometheus.MustRegister(gcConfigGauge)
	prometheus.MustRegister(gcHistogram)
}
