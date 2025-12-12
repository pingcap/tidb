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

package dxfmetric

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespaceTiDB = "tidb"
	subsystemDXF  = "dxf"
	lblType       = "type"
	lblEvent      = "event"
	lblState      = "state"

	// LblTaskID is the label for task ID.
	LblTaskID = "task_id"
)

// event names during schedule and execute
const (
	EventSubtaskScheduledAway = "subtask-scheduled-away"
	EventSubtaskRerun         = "subtask-rerun"
	EventSubtaskSlow          = "subtask-slow"
	EventRetry                = "retry"
	EventTooManyIdx           = "too-many-idx"
	EventMergeSort            = "merge-sort"
)

// DXF metrics
var (
	UsedSlotsGauge       *prometheus.GaugeVec
	WorkerCount          *prometheus.GaugeVec
	FinishedTaskCounter  *prometheus.CounterVec
	ScheduleEventCounter *prometheus.CounterVec
	ExecuteEventCounter  *prometheus.CounterVec
)

// InitDistTaskMetrics initializes disttask metrics.
func InitDistTaskMetrics() {
	UsedSlotsGauge = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespaceTiDB,
			Subsystem: "disttask",
			Name:      "used_slots",
			Help:      "Gauge of used slots on a executor node.",
		}, []string{"service_scope"})
	WorkerCount = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespaceTiDB,
			Subsystem: subsystemDXF,
			Name:      "worker_count",
			Help:      "Gauge of DXF worker count.",
		}, []string{lblType})
	FinishedTaskCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespaceTiDB,
			Subsystem: subsystemDXF,
			Name:      "finished_task_total",
			Help:      "Counter of finished DXF tasks.",
		}, []string{lblState})
	ScheduleEventCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespaceTiDB,
			Subsystem: subsystemDXF,
			Name:      "schedule_event_total",
			Help:      "Counter of DXF schedule events fo tasks.",
		}, []string{LblTaskID, lblEvent})
	// we use task ID instead of subtask ID to avoid too many lines of metrics.
	ExecuteEventCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespaceTiDB,
			Subsystem: subsystemDXF,
			Name:      "execute_event_total",
			Help:      "Counter of DXF execute events fo tasks.",
		}, []string{LblTaskID, lblEvent})
}

// Register registers DXF metrics.
func Register(register prometheus.Registerer) {
	register.MustRegister(UsedSlotsGauge)
	register.MustRegister(WorkerCount)
	register.MustRegister(FinishedTaskCounter)
	register.MustRegister(ScheduleEventCounter)
	register.MustRegister(ExecuteEventCounter)
}
