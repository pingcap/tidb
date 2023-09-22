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
	"github.com/prometheus/client_golang/prometheus"
)

const (
	DispatchingStatus = "dispatching"
	WaitingStatus     = "waiting"
	RunningStatus     = "running"
	CompletedStatus   = "completed"
)

const (
	LblTaskStatus  = "status"
	LblTaskType    = "task_type"
	LblTaskID      = "task_id"
	LblSubTaskID   = "subtask_id"
	LblSchedulerID = "scheduler_id"
)

var (
	DistDDLTaskGauge          *prometheus.GaugeVec
	DistDDLTaskStarttimeGauge *prometheus.GaugeVec
)

// InitDistDDLMetrics initializes disttask metrics.
func InitDistDDLMetrics() {
	DistDDLTaskGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "dispatcher",
			Help:      "Gauge of distdispatcher.",
		}, []string{LblTaskType, LblTaskStatus})

	DistDDLTaskStarttimeGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "dispatcher_start_time",
			Help:      "Gauge of start_time of distdispatcher.",
		}, []string{LblTaskType, LblTaskStatus, LblTaskID})
}
