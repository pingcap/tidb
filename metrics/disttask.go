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
	// DistTaskSubTaskCntGauge is the gauge of dist task subtask count.
	DistTaskSubTaskCntGauge *prometheus.GaugeVec
	// DistTaskSubTaskStartTimeGauge is the gauge of dist task subtask start time.
	DistTaskSubTaskStartTimeGauge *prometheus.GaugeVec
)

// InitDistTaskMetrics initializes disttask metrics.
func InitDistTaskMetrics() {
	DistTaskSubTaskCntGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "subtask_cnt",
			Help:      "Gauge of ddl subtask count.",
		}, []string{lblTaskType, lblTaskID, lblSchedulerID, lblTaskStatus})

	DistTaskSubTaskStartTimeGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "ddl_subtask_start_time",
			Help:      "Gauge of ddl subtask start time.",
		}, []string{lblTaskType, lblTaskID, lblSchedulerID, lblTaskStatus, lblSubTaskID})
}

// IncDistTaskSubTaskCnt increases the count of dist ddl subtask.
func IncDistTaskSubTaskCnt(subtask *proto.Subtask) {
	DistTaskSubTaskCntGauge.WithLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
	).Inc()
}

// DecDistTaskSubTaskCnt decreases the count of dist ddl subtask.
func DecDistTaskSubTaskCnt(subtask *proto.Subtask) {
	DistTaskSubTaskCntGauge.WithLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
	).Dec()
}

// StartDistTaskSubTask sets the start time of dist ddl subtask.
func StartDistTaskSubTask(subtask *proto.Subtask) {
	if subtask.IsFinished() {
		return
	}
	DistTaskSubTaskStartTimeGauge.WithLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
		strconv.Itoa(int(subtask.ID)),
	).SetToCurrentTime()
}

// EndDistTaskSubTask deletes the start time of dist ddl subtask.
func EndDistTaskSubTask(subtask *proto.Subtask) {
	DistTaskSubTaskStartTimeGauge.DeleteLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
		strconv.Itoa(int(subtask.ID)),
	)
}
