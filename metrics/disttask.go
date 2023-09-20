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
	LblTaskStatus  = "status"
	LblTaskType    = "task_type"
	LblTaskID      = "task_id"
	LblSubTaskID   = "subtask_id"
	LblSchedulerID = "scheduler_id"
)

var (
	DistDDLSubTaskCntGauge      *prometheus.GaugeVec
	DistDDLSubTaskDurationGauge *prometheus.GaugeVec
)

// InitDistDDLMetrics initializes disttask metrics.
func InitDistDDLMetrics() {
	DistDDLSubTaskCntGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "ddl_subtask_cnt",
			Help:      "Gauge of ddl subtask count.",
		}, []string{LblTaskType, LblTaskID, LblSchedulerID, LblTaskStatus})

	DistDDLSubTaskDurationGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "ddl_subtask_duration",
			Help:      "Gauge of ddl subtask duration.",
		}, []string{LblTaskType, LblTaskID, LblSchedulerID, LblTaskStatus, LblSubTaskID})
}
