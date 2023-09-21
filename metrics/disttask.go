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
	DispatchingStatus = "dispatching"
	WaitingStatus     = "waiting"
	RunningStatus     = "running"
	CompletedStatus   = "completed"
	RevertingStatus   = "reverting"
)

const (
	LblTaskStatus  = "status"
	LblTaskType    = "task_type"
	LblTaskID      = "task_id"
	LblSubTaskID   = "subtask_id"
	LblSchedulerID = "scheduler_id"
)

var (
	//DistTaskHistogram      *prometheus.HistogramVec
	DistTaskDispatcherGauge          *prometheus.GaugeVec
	DistTaskDispatcherDurationGauge  *prometheus.GaugeVec
	DistTaskDispatcherStarttimeGauge *prometheus.GaugeVec

	DistDDLSubTaskCntGauge       *prometheus.GaugeVec
	DistDDLSubTaskStartTimeGauge *prometheus.GaugeVec
)

// InitDistDDLMetrics initializes disttask metrics.
func InitDistDDLMetrics() {
	DistTaskDispatcherGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "dispatcher",
			Help:      "Gauge of distdispatcher.",
		}, []string{LblTaskType, LblTaskStatus})

	DistTaskDispatcherStarttimeGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "dispatcher_start_time",
			Help:      "Gauge of start_time of distdispatcher.",
		}, []string{LblTaskType, LblTaskStatus, LblTaskID})

	DistTaskDispatcherDurationGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "dispatcher_duration",
			Help:      "Gauge of duration time (ms) of distdispatcher.",
		}, []string{LblTaskType, LblTaskStatus, LblTaskID})

	DistDDLSubTaskCntGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "ddl_subtask_cnt",
			Help:      "Gauge of ddl subtask count.",
		}, []string{LblTaskType, LblTaskID, LblSchedulerID, LblTaskStatus})

	DistDDLSubTaskStartTimeGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "disttask",
			Name:      "ddl_subtask_start_time",
			Help:      "Gauge of ddl subtask start time.",
		}, []string{LblTaskType, LblTaskID, LblSchedulerID, LblTaskStatus, LblSubTaskID})
}

func IncDistDDLSubTaskCnt(subtask *proto.Subtask) {
	DistDDLSubTaskCntGauge.WithLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
	).Inc()
}

func DecDistDDLSubTaskCnt(subtask *proto.Subtask) {
	DistDDLSubTaskCntGauge.WithLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
	).Dec()
}

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

func EndDistDDLSubTask(subtask *proto.Subtask) {
	DistDDLSubTaskStartTimeGauge.DeleteLabelValues(
		subtask.Type,
		strconv.Itoa(int(subtask.TaskID)),
		subtask.SchedulerID,
		subtask.State,
		strconv.Itoa(int(subtask.ID)),
	)
}
