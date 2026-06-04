// Copyright 2026 PingCAP, Inc.
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

package service

import (
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	statusLive = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "service_live",
		Help:      "Whether CRR status service is live (1 means true, 0 means false).",
	}, []string{"task"})
	statusReady = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "service_ready",
		Help:      "Whether CRR status service is ready (1 means true, 0 means false).",
	}, []string{"task"})
	statusState = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "service_state",
		Help:      "Current CRR service state by task and state label.",
	}, []string{"task", "state"})
	statusPhase = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "service_phase",
		Help:      "Current CRR service phase by task and phase label.",
	}, []string{"task", "phase"})
	currentRound = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "current_round",
		Help:      "Current CRR calculation round.",
	}, []string{"task"})
	lastLoopIteration = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "last_loop_iteration",
		Help:      "Last CRR loop iteration to wait downstream files synced in current phase.",
	}, []string{"task"})
	lastUpstreamCheckpoint = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "last_upstream_checkpoint",
		Help:      "Latest upstream checkpoint observed by CRR.",
	}, []string{"task"})
	safeCheckpoint = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "safe_checkpoint",
		Help:      "Current safe checkpoint(synced upstream checkpoint) of CRR.",
	}, []string{"task"})
	syncedTS = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "synced_ts",
		Help:      "Current synced ts(minimum synced flush ts across all alive stores) of CRR.",
	}, []string{"task"})
	aliveStoreCount = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "alive_store_count",
		Help:      "Alive store count in latest planning/advance event.",
	}, []string{"task"})
	pendingFileCount = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "pending_file_count",
		Help:      "Pending file count in current CRR round.",
	}, []string{"task"})
	consecutiveFailures = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "consecutive_failures",
		Help:      "Consecutive failure count in CRR service.",
	}, []string{"task"})
	upstreamReadMetaFileCount = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "upstream_read_meta_file_count",
		Help:      "Read upstream meta file count in latest round statistic.",
	}, []string{"task"})
	estimatedSyncLogFileCount = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "estimated_sync_log_file_count",
		Help:      "Estimated sync log file count in latest round statistic.",
	}, []string{"task"})
	downstreamCheckFileCount = metricscommon.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "br_crr",
		Name:      "downstream_check_file_count",
		Help:      "Downstream check file count in latest round statistic.",
	}, []string{"task"})
)

func init() {
	prometheus.MustRegister(statusLive)
	prometheus.MustRegister(statusReady)
	prometheus.MustRegister(statusState)
	prometheus.MustRegister(statusPhase)
	prometheus.MustRegister(currentRound)
	prometheus.MustRegister(lastLoopIteration)
	prometheus.MustRegister(lastUpstreamCheckpoint)
	prometheus.MustRegister(safeCheckpoint)
	prometheus.MustRegister(syncedTS)
	prometheus.MustRegister(aliveStoreCount)
	prometheus.MustRegister(pendingFileCount)
	prometheus.MustRegister(consecutiveFailures)
	prometheus.MustRegister(upstreamReadMetaFileCount)
	prometheus.MustRegister(estimatedSyncLogFileCount)
	prometheus.MustRegister(downstreamCheckFileCount)
}

func observeStatusMetrics(snapshot *StatusSnapshot) {
	for _, state := range []string{
		stateStarting, stateRunning, stateDegraded, stateStopped,
	} {
		v := 0.0
		if state == snapshot.State {
			v = 1
		}
		statusState.WithLabelValues(snapshot.TaskName, state).Set(v)
	}
	for _, phase := range []string{
		phaseIdle,
		string(checkpoint.EventWaitingUpstream),
		string(checkpoint.EventUpstreamAdvanced),
		string(checkpoint.EventRoundPlanned),
		string(checkpoint.EventWaitingDownstream),
		string(checkpoint.EventCheckpointAdvanced),
		string(checkpoint.EventCalculationFailed),
	} {
		v := 0.0
		if phase == snapshot.Phase {
			v = 1
		}
		statusPhase.WithLabelValues(snapshot.TaskName, phase).Set(v)
	}

	statusLive.WithLabelValues(snapshot.TaskName).Set(boolToFloat(snapshot.Live))
	statusReady.WithLabelValues(snapshot.TaskName).Set(boolToFloat(snapshot.Ready))
	currentRound.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.CurrentRound))
	lastLoopIteration.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.LastLoopIteration))
	lastUpstreamCheckpoint.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.LastUpstreamCheckpoint))
	safeCheckpoint.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.SafeCheckpoint))
	syncedTS.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.SyncedTS))
	aliveStoreCount.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.AliveStoreCount))
	pendingFileCount.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.PendingFileCount))
	consecutiveFailures.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.ConsecutiveFailures))
	upstreamReadMetaFileCount.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.Statistic.UpstreamReadMetaFileCount))
	estimatedSyncLogFileCount.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.Statistic.EstimatedSyncLogFileCount))
	downstreamCheckFileCount.WithLabelValues(snapshot.TaskName).Set(float64(snapshot.Statistic.DownstreamCheckFileCount))
}

func boolToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
