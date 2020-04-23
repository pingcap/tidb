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
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Stats metrics.
var (
	AutoAnalyzeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "auto_analyze_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of auto analyze.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20), // 10ms ~ 1.5hours
		})

	AutoAnalyzeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "auto_analyze_total",
			Help:      "Counter of auto analyze.",
		}, []string{LblType})

	StatsInaccuracyRate = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_inaccuracy_rate",
			Help:      "Bucketed histogram of stats inaccuracy rate.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 14),
		})

	PseudoEstimation = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "pseudo_estimation_total",
			Help:      "Counter of pseudo estimation caused by outdated stats.",
		})

	DumpFeedbackCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "dump_feedback_total",
			Help:      "Counter of dumping feedback.",
		}, []string{LblType})

	UpdateStatsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "update_stats_total",
			Help:      "Counter of updating stats using feedback.",
		}, []string{LblType})

	StoreQueryFeedbackCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "store_query_feedback_total",
			Help:      "Counter of storing query feedback.",
		}, []string{LblType})

	GetStoreLimitErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "get_store_limit_token_error",
			Help:      "store token is up to the limit, probably because one of the stores is the hotspot or unavailable",
		}, []string{LblAddress, LblStore})

	SignificantFeedbackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "high_error_rate_feedback_total",
			Help:      "Counter of query feedback whose actual count is much different than calculated by current statistics",
		})

	FastAnalyzeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "fast_analyze_status",
			Help:      "Bucketed histogram of some stats in fast analyze.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16),
		}, []string{LblSQLType, LblType})
)
