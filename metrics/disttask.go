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
	"strconv"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	lblTaskStatus  = "status"
	lblTaskType    = "task_type"
	lblTaskID      = "task_id"
	lblSubTaskID   = "subtask_id"
	lblSchedulerID = "scheduler_id"
)

var (
	// DistDDLSubTaskCntGauge is the gauge of dist ddl subtask count.
	DistDDLSubTaskCntGauge *prometheus.GaugeVec
	// DistDDLSubTaskStartTimeGauge is the gauge of dist ddl subtask start time.
	DistDDLSubTaskStartTimeGauge *prometheus.GaugeVec
)

// InitDistDDLMetrics initializes disttask metrics.
func InitDistDDLMetrics() {
	DistDDLSubTaskCntGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "ddl_subtask_cnt",
			Help:      "Gauge of ddl subtask count.",
		}, []string{lblTaskType, lblTaskID, lblSchedulerID, lblTaskStatus})

	DistDDLSubTaskStartTimeGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "ddl_subtask_start_time",
			Help:      "Gauge of ddl subtask start time.",
		}, []string{lblTaskType, lblTaskID, lblSchedulerID, lblTaskStatus, lblSubTaskID})
}

// IncDistDDLSubTaskCnt increases the count of dist ddl subtask.
func IncDistDDLSubTaskCnt(subtask *proto.Subtask) {
	DistDDLSubTaskCntGauge.WithLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
	).Inc()
}

// DecDistDDLSubTaskCnt decreases the count of dist ddl subtask.
func DecDistDDLSubTaskCnt(subtask *proto.Subtask) {
	DistDDLSubTaskCntGauge.WithLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
	).Dec()
}

// StartDistDDLSubTask sets the start time of dist ddl subtask.
func StartDistDDLSubTask(subtask *proto.Subtask) {
	if subtask.IsFinished() {
		return
	}
	DistDDLSubTaskStartTimeGauge.WithLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
		strconv.Itoa(int(subtask.ID)),
	).SetToCurrentTime()
}

// EndDistDDLSubTask deletes the start time of dist ddl subtask.
func EndDistDDLSubTask(subtask *proto.Subtask) {
	DistDDLSubTaskStartTimeGauge.DeleteLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
		strconv.Itoa(int(subtask.ID)),
	)
}
