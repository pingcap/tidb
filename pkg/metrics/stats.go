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
	"github.com/prometheus/client_golang/prometheus"
)

// Stats metrics.
var (
	AutoAnalyzeHistogram      prometheus.Histogram
	AutoAnalyzeCounter        *prometheus.CounterVec
	StatsInaccuracyRate       prometheus.Histogram
	PseudoEstimation          *prometheus.CounterVec
	SyncLoadCounter           prometheus.Counter
	SyncLoadTimeoutCounter    prometheus.Counter
	SyncLoadHistogram         prometheus.Histogram
	ReadStatsHistogram        prometheus.Histogram
	StatsCacheCounter         *prometheus.CounterVec
	StatsCacheGauge           *prometheus.GaugeVec
	StatsHealthyGauge         *prometheus.GaugeVec
	StatsDeltaLoadHistogram   prometheus.Histogram
	StatsDeltaUpdateHistogram prometheus.Histogram

	HistoricalStatsCounter        *prometheus.CounterVec
	PlanReplayerTaskCounter       *prometheus.CounterVec
	PlanReplayerRegisterTaskGauge prometheus.Gauge
)

// InitStatsMetrics initializes stats metrics.
func InitStatsMetrics() {
	AutoAnalyzeHistogram = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "auto_analyze_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of auto analyze.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24h
		})

	AutoAnalyzeCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "auto_analyze_total",
			Help:      "Counter of auto analyze.",
		}, []string{LblType})

	StatsInaccuracyRate = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_inaccuracy_rate",
			Help:      "Bucketed histogram of stats inaccuracy rate.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 14),
		})

	PseudoEstimation = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "pseudo_estimation_total",
			Help:      "Counter of pseudo estimation caused by outdated stats.",
		}, []string{LblType})

	SyncLoadCounter = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "sync_load_total",
			Help:      "Counter of sync load.",
		})

	SyncLoadTimeoutCounter = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "sync_load_timeout_total",
			Help:      "Counter of sync load timeout.",
		})

	SyncLoadHistogram = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "sync_load_latency_millis",
			Help:      "Bucketed histogram of latency time (ms) of sync load.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 22), // 1ms ~ 1h
		})

	ReadStatsHistogram = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "read_stats_latency_millis",
			Help:      "Bucketed histogram of latency time (ms) of stats read during sync-load.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 22), // 1ms ~ 1h
		})

	StatsHealthyGauge = NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "statistics",
		Name:      "stats_healthy",
		Help:      "Gauge of stats healthy",
	}, []string{LblType})

	HistoricalStatsCounter = NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "statistics",
		Name:      "historical_stats",
		Help:      "counter of the historical stats operation",
	}, []string{LblType, LblResult})

	PlanReplayerTaskCounter = NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "plan_replayer",
		Name:      "task",
		Help:      "counter of plan replayer captured task",
	}, []string{LblType, LblResult})

	PlanReplayerRegisterTaskGauge = NewGauge(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "plan_replayer",
		Name:      "register_task",
		Help:      "gauge of plan replayer registered task",
	})
	StatsDeltaLoadHistogram = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_delta_load_duration_seconds",
			Help:      "Bucketed histogram of processing time for the background statistics loading job",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24h
		},
	)
	StatsDeltaUpdateHistogram = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "statistics",
			Name:      "stats_delta_update_duration_seconds",
			Help:      "Bucketed histogram of processing time for the background stats_meta update job",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24h
		},
	)
}
