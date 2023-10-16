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
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	lblTaskStatus = "status"
	lblTaskType   = "task_type"
	lblTaskID     = "task_id"
	lblSubTaskID  = "subtask_id"
)

// status for task
const (
	DispatchingStatus = "dispatching"
	WaitingStatus     = "waiting"
	RunningStatus     = "running"
	CompletedStatus   = "completed"
)

var (
	//DistTaskGauge is the gauge of dist task count.
	DistTaskGauge *prometheus.GaugeVec
	//DistTaskStarttimeGauge is the gauge of dist task count.
	DistTaskStarttimeGauge *prometheus.GaugeVec
	// DistTaskSubTaskCntGauge is the gauge of dist task subtask count.
	DistTaskSubTaskCntGauge *prometheus.GaugeVec
	// DistTaskSubTaskStartTimeGauge is the gauge of dist task subtask start time.
	DistTaskSubTaskStartTimeGauge *prometheus.GaugeVec
)

// InitDistTaskMetrics initializes disttask metrics.
func InitDistTaskMetrics() {
	DistTaskGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "task_status",
			Help:      "Gauge of disttask.",
		}, []string{lblTaskType, lblTaskStatus})

	DistTaskStarttimeGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "start_time",
			Help:      "Gauge of start_time of disttask.",
		}, []string{lblTaskType, lblTaskStatus, lblTaskID})

	DistTaskSubTaskCntGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "subtask_cnt",
			Help:      "Gauge of subtask count.",
		}, []string{lblTaskType, lblTaskID, lblTaskStatus})

	DistTaskSubTaskStartTimeGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "subtask_start_time",
			Help:      "Gauge of subtask start time.",
		}, []string{lblTaskType, lblTaskID, lblTaskStatus, lblSubTaskID})
}

// IncDistTaskSubTaskCnt increases the count of dist task subtask.
func IncDistTaskSubTaskCnt(subtask *proto.Subtask) {
	DistTaskSubTaskCntGauge.WithLabelValues(
		subtask.Type.String(),
		strconv.Itoa(int(subtask.TaskID)),
		subtask.State.String(),
	).Inc()
}

// DecDistTaskSubTaskCnt decreases the count of dist task subtask.
func DecDistTaskSubTaskCnt(subtask *proto.Subtask) {
	DistTaskSubTaskCntGauge.WithLabelValues(
		subtask.Type.String(),
		strconv.Itoa(int(subtask.TaskID)),
		subtask.State.String(),
	).Dec()
}

// StartDistTaskSubTask sets the start time of dist task subtask.
func StartDistTaskSubTask(subtask *proto.Subtask) {
	DistTaskSubTaskStartTimeGauge.WithLabelValues(
		subtask.Type.String(),
		strconv.Itoa(int(subtask.TaskID)),
		subtask.State.String(),
		strconv.Itoa(int(subtask.ID)),
	).SetToCurrentTime()
}

// EndDistTaskSubTask deletes the start time of dist task subtask.
func EndDistTaskSubTask(subtask *proto.Subtask) {
	DistTaskSubTaskStartTimeGauge.DeleteLabelValues(
		subtask.Type.String(),
		strconv.Itoa(int(subtask.TaskID)),
		subtask.State.String(),
		strconv.Itoa(int(subtask.ID)),
	)
}

// UpdateMetricsForAddTask update metrics when a task is added
func UpdateMetricsForAddTask(task *proto.Task) {
	DistTaskGauge.WithLabelValues(task.Type.String(), WaitingStatus).Inc()
	DistTaskStarttimeGauge.WithLabelValues(task.Type.String(), WaitingStatus, fmt.Sprint(task.ID)).Set(float64(time.Now().UnixMicro()))
}

// UpdateMetricsForDispatchTask update metrics when a task is added
func UpdateMetricsForDispatchTask(task *proto.Task) {
	DistTaskGauge.WithLabelValues(task.Type.String(), WaitingStatus).Dec()
	DistTaskStarttimeGauge.DeleteLabelValues(task.Type.String(), WaitingStatus, fmt.Sprint(task.ID))
	DistTaskStarttimeGauge.WithLabelValues(task.Type.String(), DispatchingStatus, fmt.Sprint(task.ID)).SetToCurrentTime()
}

// UpdateMetricsForRunTask update metrics when a task starts running
func UpdateMetricsForRunTask(task *proto.Task) {
	DistTaskStarttimeGauge.DeleteLabelValues(task.Type.String(), DispatchingStatus, fmt.Sprint(task.ID))
	DistTaskGauge.WithLabelValues(task.Type.String(), DispatchingStatus).Dec()
	DistTaskGauge.WithLabelValues(task.Type.String(), RunningStatus).Inc()
}

// UpdateMetricsForFinishTask update metrics when a task is finished
func UpdateMetricsForFinishTask(task *proto.Task) {
	DistTaskGauge.WithLabelValues(task.Type.String(), RunningStatus).Dec()
	DistTaskGauge.WithLabelValues(task.Type.String(), CompletedStatus).Inc()
}
