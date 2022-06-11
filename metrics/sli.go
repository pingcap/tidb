// Copyright 2021 PingCAP, Inc.
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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// SmallTxnWriteDuration uses to collect small transaction write duration.
	SmallTxnWriteDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "sli",
			Name:      "small_txn_write_duration_seconds",
			Help:      "Bucketed histogram of small transaction write time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 74h
		})

	// TxnWriteThroughput uses to collect transaction write throughput which transaction is not small.
	TxnWriteThroughput = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "sli",
			Name:      "txn_write_throughput",
			Help:      "Bucketed histogram of transaction write throughput (bytes/second).",
			Buckets:   prometheus.ExponentialBuckets(64, 1.3, 40), // 64 bytes/s ~ 2.3MB/s
		})
)
