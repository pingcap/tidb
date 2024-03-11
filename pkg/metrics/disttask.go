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
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	lblTaskStatus = "status"
	lblTaskType   = "task_type"
	lblTaskID     = "task_id"
	lblSubTaskID  = "subtask_id"
	lblExecID     = "exec_id"
)

// status for task
const (
	SchedulingStatus = "scheduling"
	WaitingStatus    = "waiting"
	RunningStatus    = "running"
	CompletedStatus  = "completed"
)

var (
	//DistTaskGauge is the gauge of dist task count.
	DistTaskGauge *prometheus.GaugeVec
	//DistTaskStarttimeGauge is the gauge of dist task count.
	DistTaskStarttimeGauge *prometheus.GaugeVec
	// DistTaskSubTaskCntGauge is the gauge of dist task subtask count.
	DistTaskSubTaskCntGauge *prometheus.GaugeVec
	// DistTaskSubTaskDurationGauge is the gauge of dist task subtask duration.
	DistTaskSubTaskDurationGauge *prometheus.GaugeVec
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
}

// UpdateMetricsForAddTask update metrics when a task is added
func UpdateMetricsForAddTask(task *proto.TaskBase) {
	DistTaskGauge.WithLabelValues(task.Type.String(), WaitingStatus).Inc()
	DistTaskStarttimeGauge.WithLabelValues(task.Type.String(), WaitingStatus, fmt.Sprint(task.ID)).Set(float64(time.Now().UnixMicro()))
}

// UpdateMetricsForScheduleTask update metrics when a task is added
func UpdateMetricsForScheduleTask(id int64, taskType proto.TaskType) {
	DistTaskGauge.WithLabelValues(taskType.String(), WaitingStatus).Dec()
	DistTaskStarttimeGauge.DeleteLabelValues(taskType.String(), WaitingStatus, fmt.Sprint(id))
	DistTaskStarttimeGauge.WithLabelValues(taskType.String(), SchedulingStatus, fmt.Sprint(id)).SetToCurrentTime()
}

// UpdateMetricsForRunTask update metrics when a task starts running
func UpdateMetricsForRunTask(task *proto.Task) {
	DistTaskStarttimeGauge.DeleteLabelValues(task.Type.String(), SchedulingStatus, fmt.Sprint(task.ID))
	DistTaskGauge.WithLabelValues(task.Type.String(), SchedulingStatus).Dec()
	DistTaskGauge.WithLabelValues(task.Type.String(), RunningStatus).Inc()
}

// UpdateMetricsForFinishTask update metrics when a task is finished
func UpdateMetricsForFinishTask(task *proto.Task) {
	DistTaskGauge.WithLabelValues(task.Type.String(), RunningStatus).Dec()
	DistTaskGauge.WithLabelValues(task.Type.String(), CompletedStatus).Inc()
}
