package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	RestoreImportFileSeconds       prometheus.Histogram
	RestoreUploadSSTForPiTRSeconds prometheus.Histogram
	// RestoreTableCreatedCount counts how many tables created.
	RestoreTableCreatedCount prometheus.Counter
)

func InitBRMetrics() {
	RestoreTableCreatedCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "BR",
		Name:      "table_created",
		Help:      "The count of tables have been created.",
	})

	RestoreImportFileSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "br",
		Name:      "restore_import_file_seconds",

		Help: "The time cost for importing a file. (including the time costed in queuing)",

		Buckets: prometheus.ExponentialBuckets(0.01, 4, 14),
	})

	RestoreUploadSSTForPiTRSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "br",
		Name:      "restore_upload_sst_for_pitr_seconds",

		Help: "The time cost for uploading SST files for point-in-time recovery",

		Buckets: prometheus.DefBuckets,
	})
}
