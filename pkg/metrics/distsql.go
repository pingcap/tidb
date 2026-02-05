// Copyright 2018 PingCAP, Inc.
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
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// distsql metrics.
var (
	DistSQLQueryHistogram           *prometheus.HistogramVec
	DistSQLScanKeysPartialHistogram prometheus.Histogram
	DistSQLScanKeysHistogram        prometheus.Histogram
	DistSQLPartialCountHistogram    prometheus.Histogram
	DistSQLCoprCacheCounter         *prometheus.CounterVec
	DistSQLCoprBucketSplitFallback  prometheus.Counter
	DistSQLCoprClosestReadCounter   *prometheus.CounterVec
	DistSQLCoprRespBodySize         *prometheus.HistogramVec
)

// InitDistSQLMetrics initializes distsql metrics.
func InitDistSQLMetrics() {
	DistSQLQueryHistogram = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "distsql",
			Name:      "handle_query_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblSQLType, LblCoprType})

	DistSQLScanKeysPartialHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "distsql",
			Name:      "scan_keys_partial_num",
			Help:      "number of scanned keys for each partial result.",
		},
	)

	DistSQLScanKeysHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "distsql",
			Name:      "scan_keys_num",
			Help:      "number of scanned keys for each query.",
		},
	)

	DistSQLPartialCountHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "distsql",
			Name:      "partial_num",
			Help:      "number of partial results for each query.",
		},
	)

	DistSQLCoprCacheCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "distsql",
			Name:      "copr_cache",
			Help:      "coprocessor cache hit, evict and miss number",
		}, []string{LblType})

	DistSQLCoprBucketSplitFallback = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "distsql",
			Name:      "copr_bucket_split_fallback",
			Help:      "counter of copr bucket split falling back to region-only splitting",
		},
	)

	DistSQLCoprClosestReadCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "distsql",
			Name:      "copr_closest_read",
			Help:      "counter of total copr read local read hit.",
		}, []string{LblType})

	DistSQLCoprRespBodySize = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "distsql",
			Name:      "copr_resp_size",
			Help:      "copr task response data size in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		}, []string{LblStore})
}
