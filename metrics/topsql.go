package metrics

import "github.com/prometheus/client_golang/prometheus"

// Top SQL metrics.
var (
	RegisterSQLFail = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "register_sql_fail_total",
			Help:      "Counter of top-sql register SQL failed",
		})
	RegisterPlanFail = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "register_plan_fail_total",
			Help:      "Counter of top-sql register plan failed",
		})
	IgnoreCollectCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "ignore_collect_total",
			Help:      "Counter of top-sql ignore collect records cause by collect worker too slow",
		})
	IgnoreReportCnt = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "ignore_report_total",
			Help:      "Counter of top-sql ignore report records cause by report worker too slow",
		})
	ReportDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "topsql",
			Name:      "report_duration_seconds",
			Help:      "Bucketed histogram of reporting time (s) of top-sql agent do report",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblResult})
)
