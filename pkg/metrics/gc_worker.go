// Copyright 2017 PingCAP, Inc.
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
	"github.com/tikv/client-go/v2/metrics"
)

// Metrics for the GC worker.
var (
	GCWorkerCounter                        *prometheus.CounterVec
	GCHistogram                            *prometheus.HistogramVec
	GCConfigGauge                          *prometheus.GaugeVec
	GCJobFailureCounter                    *prometheus.CounterVec
	GCActionRegionResultCounter            *prometheus.CounterVec
	GCRegionTooManyLocksCounter            prometheus.Counter
	GCUnsafeDestroyRangeFailuresCounterVec *prometheus.CounterVec
)

// InitGCWorkerMetrics initializes GC worker metrics.
func InitGCWorkerMetrics() {
	GCWorkerCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_worker_actions_total",
			Help:      "Counter of gc worker actions.",
		}, []string{"type"})

	GCHistogram = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_seconds",
			Help:      "Bucketed histogram of gc duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1s ~ 6days
		}, []string{"stage"})

	GCConfigGauge = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_config",
			Help:      "Gauge of GC configs.",
		}, []string{"type"})

	GCJobFailureCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_failure",
			Help:      "Counter of gc job failure.",
		}, []string{"type"})

	GCActionRegionResultCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_action_result",
			Help:      "Counter of gc action result on region level.",
		}, []string{"type"})

	GCRegionTooManyLocksCounter = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_region_too_many_locks",
			Help:      "Counter of gc scan lock request more than once in the same region.",
		})
}

// StageTotal is used in the "stage" label of GCHistogram to represent the total time of a turn of GC.
const StageTotal = "total"

func init() {
	GCUnsafeDestroyRangeFailuresCounterVec = metrics.TiKVUnsafeDestroyRangeFailuresCounterVec
}
