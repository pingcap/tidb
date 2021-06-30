package metrics

import "github.com/prometheus/client_golang/prometheus"

// Top SQL metrics.
var (
	TopSQLRegisterFail = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "register_fail_total",
			Help:      "Counter of top-sql register sql/plan failed",
		}, []string{LblType})
	TopSQLIgnoreCnt = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "ignore_total",
			Help:      "Counter of top-sql ignore collect/report records cause by collect/report worker too slow",
		}, []string{LblType})
	TopSQLReportDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "report_duration_seconds",
			Help:      "Bucketed histogram of reporting time (s) of top-sql agent do report",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblResult})
)
