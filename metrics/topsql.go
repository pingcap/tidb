package metrics

import "github.com/prometheus/client_golang/prometheus"

// Top SQL metrics.
var (
	TopSQLIgnoredCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "ignored_total",
			Help:      "Counter of ignored top-sql metrics (register-sql, register-plan, collect-data and report-data), normally it should be 0.",
		}, []string{LblType})

	TopSQLReportDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "report_duration_seconds",
			Help:      "Bucket histogram of reporting time (s) to the top-sql agent",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 24), // 1ms ~ 2.3h
		}, []string{LblType, LblResult})

	TopSQLReportDataHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "report_data_total",
			Help:      "Bucket histogram of reporting records/sql/plan count to the top-sql agent.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1 ~ 524288
		}, []string{LblType})
)
