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
	// RawKVBatchPutDurationSeconds records the time cost for batch put
	RawKVBatchPutDurationSeconds *prometheus.HistogramVec
	// RawKVBatchPutBatchSize records the number of kv entries in the batch put
	RawKVBatchPutBatchSize *prometheus.HistogramVec
)

// InitRawKVMetrics initializes all metrics in rawkv.
func InitRawKVMetrics() {
	RawKVBatchPutDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "rawkv",
		Name:      "rawkv_batch_put_duration_seconds",
		Help:      "The time cost batch put kvs",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 17), // 1ms ~ 1min
	}, []string{"cf"})

	RawKVBatchPutBatchSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "rawkv",
		Name:      "rawkv_batch_put_batch_size",
		Help:      "Number of kv entries in the batch put",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 9), // 1 ~ 256
	}, []string{"cf"})
}
