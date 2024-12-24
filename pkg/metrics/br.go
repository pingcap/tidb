package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	RestoreImportFileSeconds       prometheus.Histogram
	RestoreUploadSSTForPiTRSeconds prometheus.Histogram
)

func InitBRMetrics() {
	RestoreImportFileSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "br",
		Name:      "restore_import_file_seconds",

		Help: "The time cost for importing a file (send it to a file's all peers and put metadata)",

		Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
	})

	RestoreUploadSSTForPiTRSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "br",
		Name:      "restore_upload_sst_for_pitr_seconds",

		Help: "The time cost for uploading SST files for point-in-time recovery",

		Buckets: prometheus.DefBuckets,
	})
}
