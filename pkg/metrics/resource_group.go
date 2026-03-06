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

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics
// Query duration by query is QueryDurationHistogram in `server.go`.
var (
	RunawayCheckerCounter *prometheus.CounterVec

	RunawayFlusherCounter            *prometheus.CounterVec
	RunawayFlusherAddCounter         *prometheus.CounterVec
	RunawayFlusherBatchSizeHistogram *prometheus.HistogramVec
	RunawayFlusherDurationHistogram  *prometheus.HistogramVec
	RunawayFlusherIntervalHistogram  *prometheus.HistogramVec
)

// InitResourceGroupMetrics initializes resource group metrics.
func InitResourceGroupMetrics() {
	RunawayCheckerCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "query_runaway_check",
			Help:      "Counter of query triggering runaway check.",
		}, []string{LblResourceGroup, LblType, LblAction})

	RunawayFlusherCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "runaway_flusher_total",
			Help:      "Counter of runaway flusher operations.",
		}, []string{LblName, LblResult})

	RunawayFlusherAddCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "runaway_flusher_add_total",
			Help:      "Counter of records added to runaway flusher.",
		}, []string{LblName})

	RunawayFlusherBatchSizeHistogram = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "runaway_flusher_batch_size",
			Help:      "Batch size of runaway flusher operations.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1, 2, 4, ..., 512
		}, []string{LblName})

	RunawayFlusherDurationHistogram = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "runaway_flusher_duration_seconds",
			Help:      "Duration of runaway flusher operations in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms ~ 16s
		}, []string{LblName})

	RunawayFlusherIntervalHistogram = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "runaway_flusher_interval_seconds",
			Help:      "Interval between runaway flusher operations in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s ~ 200s
		}, []string{LblName})
}
