// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// RestoreImportFileSeconds records the time cost for importing a file.
	// Including download / queuing.
	RestoreImportFileSeconds prometheus.Histogram
	// RestoreUploadSSTForPiTRSeconds records the time cost for uploading SST
	// files during restoring for future PiTR.
	RestoreUploadSSTForPiTRSeconds prometheus.Histogram
	// RestoreUploadSSTMetaForPiTRSeconds records the time cost for saving metadata
	// of uploaded SSTs for future PiTR.
	RestoreUploadSSTMetaForPiTRSeconds prometheus.Histogram

	// RestoreTableCreatedCount counts how many tables created.
	RestoreTableCreatedCount prometheus.Counter
)

// InitBRMetrics initializes all metrics in BR.
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

		Help: "The time cost for importing a file. (including the time cost in queuing)",

		Buckets: prometheus.ExponentialBuckets(0.01, 4, 14),
	})

	RestoreUploadSSTForPiTRSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "br",
		Name:      "restore_upload_sst_for_pitr_seconds",

		Help: "The time cost for uploading SST files for point-in-time recovery",

		Buckets: prometheus.DefBuckets,
	})

	RestoreUploadSSTMetaForPiTRSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "br",
		Name:      "restore_upload_sst_meta_for_pitr_seconds",

		Help:    "The time cost for uploading SST metadata for point-in-time recovery",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
	})
}
