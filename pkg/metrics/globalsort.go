// Copyright 2023 PingCAP, Inc.
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
	// GlobalSortWriteToCloudStorageDuration records the duration of writing to cloud storage.
	GlobalSortWriteToCloudStorageDuration *prometheus.HistogramVec
	// GlobalSortWriteToCloudStorageRate records the rate of writing to cloud storage.
	GlobalSortWriteToCloudStorageRate *prometheus.HistogramVec
	// GlobalSortReadFromCloudStorageDuration records the duration of reading from cloud storage.
	GlobalSortReadFromCloudStorageDuration *prometheus.HistogramVec
	// GlobalSortReadFromCloudStorageRate records the rate of reading from cloud storage.
	GlobalSortReadFromCloudStorageRate *prometheus.HistogramVec
	// GlobalSortIngestWorkerCnt records the working number of ingest workers.
	GlobalSortIngestWorkerCnt *prometheus.GaugeVec
	// GlobalSortUploadWorkerCount is the gauge of active parallel upload worker count.
	GlobalSortUploadWorkerCount prometheus.Gauge
)

// InitGlobalSortMetrics initializes defines global sort metrics.
func InitGlobalSortMetrics() {
	GlobalSortWriteToCloudStorageDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "global_sort",
		Name:      "write_to_cloud_storage_duration",
		Help:      "write to cloud storage duration",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
	}, []string{LblType})

	GlobalSortWriteToCloudStorageRate = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "global_sort",
		Name:      "write_to_cloud_storage_rate",
		Help:      "write to cloud storage rate",
		Buckets:   prometheus.ExponentialBuckets(0.05, 2, 20),
	}, []string{LblType})

	GlobalSortReadFromCloudStorageDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "global_sort",
		Name:      "read_from_cloud_storage_duration",
		Help:      "read from cloud storage duration",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{LblType})

	GlobalSortReadFromCloudStorageRate = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "global_sort",
		Name:      "read_from_cloud_storage_rate",
		Help:      "read from cloud storage rate",
		Buckets:   prometheus.ExponentialBuckets(0.05, 2, 20),
	}, []string{LblType})

	GlobalSortIngestWorkerCnt = NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "global_sort",
		Name:      "ingest_worker_cnt",
		Help:      "ingest worker cnt",
	}, []string{LblType})

	GlobalSortUploadWorkerCount = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "global_sort",
			Name:      "upload_worker_cnt",
			Help:      "Gauge of active parallel upload worker count.",
		},
	)
}
