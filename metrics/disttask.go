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

	"github.com/pingcap/tidb/disttask/framework/proto"
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
	DistTaskGauge          *prometheus.GaugeVec
	DistTaskStarttimeGauge *prometheus.GaugeVec
)

// InitDistDDLMetrics initializes disttask metrics.
func InitDistTaskMetrics() {
	DistTaskGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "task_status",
			Help:      "Gauge of disttask.",
		}, []string{LblTaskType, LblTaskStatus})

	DistTaskStarttimeGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "start_time",
			Help:      "Gauge of start_time of disttask.",
		}, []string{LblTaskType, LblTaskStatus, LblTaskID})
}

func UpdateMetricsForAddTask(task *proto.Task) {
	DistTaskGauge.WithLabelValues(task.Type, WaitingStatus).Inc()
	DistTaskStarttimeGauge.WithLabelValues(task.Type, WaitingStatus, fmt.Sprint(task.ID)).Set(float64(time.Now().UnixMicro()))
}

func UpdateMetricsForDisptchTask(task *proto.Task) {
	DistTaskGauge.WithLabelValues(task.Type, WaitingStatus).Dec()
	DistTaskStarttimeGauge.DeleteLabelValues(task.Type, WaitingStatus, fmt.Sprint(task.ID))
	DistTaskStarttimeGauge.WithLabelValues(task.Type, DispatchingStatus, fmt.Sprint(task.ID)).SetToCurrentTime()
}

func UpdateMetricsForRunTask(task *proto.Task) {
	DistTaskStarttimeGauge.DeleteLabelValues(task.Type, DispatchingStatus, fmt.Sprint(task.ID))
	DistTaskGauge.WithLabelValues(task.Type, DispatchingStatus).Dec()
	DistTaskGauge.WithLabelValues(task.Type, RunningStatus).Inc()
}

func UpdateMetricsForFinishTask(task *proto.Task) {
	DistTaskGauge.WithLabelValues(task.Type, RunningStatus).Dec()
	DistTaskGauge.WithLabelValues(task.Type, CompletedStatus).Inc()
}
