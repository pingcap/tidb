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

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TempDirWriteStorageRate records the rate of writing to temp-dir.
	TempDirWriteStorageRate *prometheus.HistogramVec
	// TempDirReadStorageRate records the rate of reading from temp-dir.
	TempDirReadStorageRate *prometheus.HistogramVec
	// TempDirTotalFilesCount records the total number of files in temp-dir.
	TempDirTotalFilesCount *prometheus.GaugeVec
	// TempDirTotalFilesSize records the total size of files in temp-dir.
	TempDirTotalFilesSize *prometheus.GaugeVec
)

// InitTempDirMetrics initializes the temp directory metrics.
func InitTempDirMetrics() {
	TempDirWriteStorageRate = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "temp_dir",
			Name:      "write_rate_bytes_per_second",
			Help:      "The rate of writing kv to the temp storage directory in bytes per second.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		}, []string{LblType})

	TempDirReadStorageRate = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "temp_dir",
			Name:      "read_rate_bytes_per_second",
			Help:      "The rate of reading kv from the temp storage directory in bytes per second.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		}, []string{LblType})

	TempDirTotalFilesCount = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "temp_dir",
			Name:      "total_files_count",
			Help:      "The total number of files in the temp storage directory.",
		}, []string{LblType})

	TempDirTotalFilesSize = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "temp_dir",
			Name:      "total_files_size_mega_bytes",
			Help:      "The total size of files in the temp storage directory in mebibyte.",
		}, []string{LblType})
}

//revive:disable:exported
var CountFilesAndSize func(root string) (count int, size int) = nil
var GetIngestTempDataDir func() string = nil
var GetImportTempDataDir func() string = nil

//revive:enable:exported

func observeTempDirUsage(label string) {
	count, size := CountFilesAndSize(label)
	TempDirTotalFilesCount.WithLabelValues(label).Set(float64(count))
	TempDirTotalFilesSize.WithLabelValues(label).Set(float64(size) / 1024.0 / 1024.0)
}

// startTempDirUsageObserver starts a goroutine that periodically updates temp directory usage metrics
// for the ingest and import directories every 30 seconds.
func startTempDirUsageObserver() {
	ingestDir := GetIngestTempDataDir()
	importDir := GetImportTempDataDir()
	ctx := context.Background()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				observeTempDirUsage(ingestDir)
				observeTempDirUsage(importDir)
			}
		}
	}()
}
