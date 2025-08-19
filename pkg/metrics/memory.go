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
	"sync"

	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "tidb"
	subsystem = "memory"
)

// Memory metrics.
var (
	GlobalMemArbitrationDuration         prometheus.Histogram
	GlobalMemArbitratorWorkMode          prometheus.GaugeVec
	GlobalMemArbitratorQuota             prometheus.GaugeVec
	GlobalMemArbitratorWaitingTask       prometheus.GaugeVec
	GlobalMemArbitratorRuntimeMemMagnifi prometheus.Gauge
	GlobalMemArbitratorRootPool          prometheus.GaugeVec

	GlobalMemArbitratorActionCount prometheus.CounterVec
	GlobalMemArbitratorAction      struct {
		PoolInitHitDigest   prometheus.Counter
		PoolInitReserve     prometheus.Counter
		PoolInitMediumQuota prometheus.Counter
		PoolInitNone        prometheus.Counter
	}

	GlobalMemArbitratorTaskExecCount prometheus.CounterVec
	GlobalMemArbitratorTaskExec      struct {
		CancelWaitAverseParse   prometheus.Counter
		CancelWaitAversePlan    prometheus.Counter
		CancelStandardModeParse prometheus.Counter
		CancelStandardModePlan  prometheus.Counter
		ForceKillParse          prometheus.Counter
		ForceKillPlan           prometheus.Counter
	}

	counters struct {
		c map[string]prometheus.Counter
		sync.RWMutex
	}
	gauges struct {
		g map[string]prometheus.Gauge
		sync.RWMutex
	}
)

// InitMemoryMetrics initializes the memory metrics for the global memory arbitrator.
func InitMemoryMetrics() {
	GlobalMemArbitrationDuration = metricscommon.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "arbitration_duration_seconds",
		Help:      "Bucketed histogram of mem quota arbitration time (s) in SQL execution",
		Buckets:   prometheus.ExponentialBuckets(0.00005, 4, 18),
	})
	GlobalMemArbitratorQuota = *metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "arbitrator_quota_bytes",
		Help:      "Quota info of the global memory arbitrator",
	}, []string{LblType})
	GlobalMemArbitratorWorkMode = *metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "arbitrator_work_mode",
		Help:      "Work mode of the global memory arbitrator",
	}, []string{LblType})
	GlobalMemArbitratorWaitingTask = *metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "arbitrator_waiting_task",
		Help:      "Waiting task num of the global memory arbitrator",
	}, []string{LblType})
	GlobalMemArbitratorRuntimeMemMagnifi = metricscommon.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "arbitrator_magnifi_ratio",
		Help:      "Runtime profile (heapinuse vs. quota) of the global memory arbitrator",
	})
	GlobalMemArbitratorRootPool = *metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "arbitrator_root_pool",
		Help:      "Root pool info of the global memory arbitrator",
	}, []string{LblType})
	GlobalMemArbitratorTaskExecCount = *metricscommon.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "arbitrator_task_exec",
		Help:      "Task execution count of the global memory arbitrator",
	}, []string{LblType})
	GlobalMemArbitratorActionCount = *metricscommon.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "arbitrator_action",
		Help:      "Action count of the global memory arbitrator",
	}, []string{LblType})

	GlobalMemArbitratorAction.PoolInitHitDigest = GlobalMemArbitratorActionCount.WithLabelValues("pool-init-hit-digest")
	GlobalMemArbitratorAction.PoolInitReserve = GlobalMemArbitratorActionCount.WithLabelValues("pool-init-reserve")
	GlobalMemArbitratorAction.PoolInitMediumQuota = GlobalMemArbitratorActionCount.WithLabelValues("pool-init-medium-quota")
	GlobalMemArbitratorAction.PoolInitNone = GlobalMemArbitratorActionCount.WithLabelValues("pool-init-none")

	GlobalMemArbitratorTaskExec.CancelWaitAverseParse = GlobalMemArbitratorTaskExecCount.WithLabelValues("cancel-wait-averse-parse")
	GlobalMemArbitratorTaskExec.CancelWaitAversePlan = GlobalMemArbitratorTaskExecCount.WithLabelValues("cancel-wait-averse-plan")
	GlobalMemArbitratorTaskExec.CancelStandardModeParse = GlobalMemArbitratorTaskExecCount.WithLabelValues("cancel-standard-mode-parse")
	GlobalMemArbitratorTaskExec.CancelStandardModePlan = GlobalMemArbitratorTaskExecCount.WithLabelValues("cancel-standard-mode-plan")
	GlobalMemArbitratorTaskExec.ForceKillParse = GlobalMemArbitratorTaskExecCount.WithLabelValues("force-kill-parse")
	GlobalMemArbitratorTaskExec.ForceKillPlan = GlobalMemArbitratorTaskExecCount.WithLabelValues("force-kill-plan")
}

// AddGlobalMemArbitratorCounter adds a counter for the global memory arbitrator.
func AddGlobalMemArbitratorCounter(counterVec prometheus.CounterVec, taskType string, count int64) {
	var c prometheus.Counter
	{
		counters.RLock()
		c = counters.c[taskType]
		counters.RUnlock()
	}

	if c == nil {
		c = counterVec.WithLabelValues(taskType)
		{
			counters.Lock()

			if counters.c == nil {
				counters.c = make(map[string]prometheus.Counter)
			}
			counters.c[taskType] = c

			counters.Unlock()
		}
	}

	c.Add(float64(count))
}

// SetGlobalMemArbitratorGauge sets a gauge for the global memory arbitrator.
func SetGlobalMemArbitratorGauge(gaugeVec prometheus.GaugeVec, taskType string, value int64) {
	var g prometheus.Gauge
	{
		gauges.RLock()
		g = gauges.g[taskType]
		gauges.RUnlock()
	}

	if g == nil {
		g = gaugeVec.WithLabelValues(taskType)
		{
			gauges.Lock()

			if gauges.g == nil {
				gauges.g = make(map[string]prometheus.Gauge)
			}
			gauges.g[taskType] = g

			gauges.Unlock()
		}
	}

	g.Set(float64(value))
}
