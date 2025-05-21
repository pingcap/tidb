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

	// MetaKVBatchFiles counts how many meta KV files restored in the batch
	MetaKVBatchFiles *prometheus.HistogramVec
	// MetaKVBatchFilteredKeys counts how many meta KV entries filtered from the batch
	MetaKVBatchFilteredKeys *prometheus.HistogramVec
	// MetaKVBatchKeys counts how many meta KV entries restored in the batch
	MetaKVBatchKeys *prometheus.HistogramVec
	// MetaKVBatchSize records the total size of the meta KV entries restored in the batch
	MetaKVBatchSize *prometheus.HistogramVec

	// KVApplyBatchDuration records the duration to apply the batch of KV files
	KVApplyBatchDuration prometheus.Histogram
	// KVApplyBatchFiles counts how many KV files restored in the batch
	KVApplyBatchFiles prometheus.Histogram
	// KVApplyBatchRegions counts how many regions restored in the batch of KV files
	KVApplyBatchRegions prometheus.Histogram
	// KVApplyBatchSize records the total size of the KV files restored in the batch
	KVApplyBatchSize prometheus.Histogram
	// KVApplyRegionFiles counts how many KV files restored for a region
	KVApplyRegionFiles prometheus.Histogram
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

	MetaKVBatchFiles = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "meta_kv_batch_files",
			Help:      "The number of meta KV files in the batch",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12), // 1 ~ 2048
		}, []string{"cf"})
	MetaKVBatchFilteredKeys = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "meta_kv_batch_filtered_keys",
			Help:      "The number of filtered meta KV entries from the batch",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18), // 1 ~ 128Ki
		}, []string{"cf"})
	MetaKVBatchKeys = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "meta_kv_batch_keys",
			Help:      "The number of meta KV entries in the batch",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18), // 1 ~ 128Ki
		}, []string{"cf"})
	MetaKVBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "meta_kv_batch_size",
			Help:      "The total size of meta KV entries in the batch",
			Buckets:   prometheus.ExponentialBuckets(256, 2, 20), // 256 ~ 128Mi
		}, []string{"cf"})

	KVApplyBatchDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "kv_apply_batch_duration_seconds",
			Help:      "The duration to apply the batch of KV files",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 21), // 1ms ~ 15min
		})
	KVApplyBatchFiles = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "kv_apply_batch_files",
			Help:      "The number of KV files in the batch",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 11), // 1 ~ 1024
		})
	KVApplyBatchRegions = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "kv_apply_batch_regions",
			Help:      "The number of regions in the range of entries in the batch of KV files",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12), // 1 ~ 2048
		})
	KVApplyBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "kv_apply_batch_size",
			Help:      "The number of KV files in the batch",
			Buckets:   prometheus.ExponentialBuckets(1024, 2, 21), // 1KiB ~ 1GiB
		})
	KVApplyRegionFiles = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "br",
			Name:      "kv_apply_region_files",
			Help:      "The number of KV files restored for a region",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 11), // 1 ~ 1024
		})
}
