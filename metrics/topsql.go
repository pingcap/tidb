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
			Help:      "Bucket histogram of reporting gRPC duration to the top-sql agent",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblResult})

	TopSQLReportDataTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "report_data_total",
			Help:      "Counter of top-sql report records/sql/plan data.",
		}, []string{LblType})
)
