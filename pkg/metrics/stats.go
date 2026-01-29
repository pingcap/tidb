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

// Stats metrics.
var (
	AutoAnalyzeHistogram      prometheus.Histogram
	AutoAnalyzeCounter        *prometheus.CounterVec
	ManualAnalyzeCounter      *prometheus.CounterVec
	StatsInaccuracyRate       prometheus.Histogram
	PseudoEstimation          *prometheus.CounterVec
	SyncLoadCounter           prometheus.Counter
	SyncLoadTimeoutCounter    prometheus.Counter
	SyncLoadDedupCounter      prometheus.Counter
	SyncLoadHistogram         prometheus.Histogram
	ReadStatsHistogram        prometheus.Histogram
	StatsCacheCounter         *prometheus.CounterVec
	StatsCacheGauge           *prometheus.GaugeVec
	StatsHealthyGauge         *prometheus.GaugeVec
	StatsDeltaLoadHistogram   prometheus.Histogram
	StatsDeltaUpdateHistogram prometheus.Histogram
	StatsUsageUpdateHistogram prometheus.Histogram

	HistoricalStatsCounter        *prometheus.CounterVec
	PlanReplayerTaskCounter       *prometheus.CounterVec
	PlanReplayerRegisterTaskGauge prometheus.Gauge
)

// InitStatsMetrics initializes stats metrics.
func InitStatsMetrics() {
	AutoAnalyzeHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "auto_analyze_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of auto analyze.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24h
		})

	AutoAnalyzeCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "auto_analyze_total",
			Help:      "Counter of auto analyze.",
		}, []string{LblType})

	ManualAnalyzeCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "manual_analyze_total",
			Help:      "Counter of manual analyze.",
		}, []string{LblType})

	StatsInaccuracyRate = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_inaccuracy_rate",
			Help:      "Bucketed histogram of stats inaccuracy rate.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 14),
		})

	PseudoEstimation = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "pseudo_estimation_total",
			Help:      "Counter of pseudo estimation caused by outdated stats.",
		}, []string{LblType})

	SyncLoadCounter = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "sync_load_total",
			Help:      "Counter of sync load.",
		})

	SyncLoadTimeoutCounter = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "sync_load_timeout_total",
			Help:      "Counter of sync load timeout.",
		})
	SyncLoadDedupCounter = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "sync_load_dedup_total",
			Help:      "Counter of deduplicated sync load.",
		})

	SyncLoadHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "sync_load_latency_millis",
			Help:      "Bucketed histogram of latency time (ms) of sync load.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 22), // 1ms ~ 1h
		})

	ReadStatsHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "read_stats_latency_millis",
			Help:      "Bucketed histogram of latency time (ms) of stats read during sync-load.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 22), // 1ms ~ 1h
		})

	StatsHealthyGauge = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "statistics",
		Name:      "stats_healthy",
		Help:      "Gauge of stats healthy",
	}, []string{LblType})

	HistoricalStatsCounter = metricscommon.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "statistics",
		Name:      "historical_stats",
		Help:      "counter of the historical stats operation",
	}, []string{LblType, LblResult})

	PlanReplayerTaskCounter = metricscommon.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "plan_replayer",
		Name:      "task",
		Help:      "counter of plan replayer captured task",
	}, []string{LblType, LblResult})

	PlanReplayerRegisterTaskGauge = metricscommon.NewGauge(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "plan_replayer",
		Name:      "register_task",
		Help:      "gauge of plan replayer registered task",
	})
	StatsDeltaLoadHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_delta_load_duration_seconds",
			Help:      "Bucketed histogram of processing time for the background statistics loading job",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24h
		},
	)
	StatsDeltaUpdateHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_delta_update_duration_seconds",
			Help:      "Bucketed histogram of processing time for the background stats_meta update job",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24h
		},
	)
	StatsUsageUpdateHistogram = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_usage_update_duration_seconds",
			Help:      "Bucketed histogram of processing time for the background stats usage update job",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24h
		},
	)
	StatsCacheCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_cache_op",
			Help:      "Counter for statsCache operation",
		}, []string{LblType})
	StatsCacheGauge = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "statistics",
		Name:      "stats_cache_val",
		Help:      "gauge of stats cache value",
	}, []string{LblType})
}
