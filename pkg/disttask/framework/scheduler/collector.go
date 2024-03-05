// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/prometheus/client_golang/prometheus"
)

var subtaskCollector = newCollector()

func init() {
	prometheus.MustRegister(subtaskCollector)
}

// Because the exec_id of a subtask may change, after all tasks
// are successful, subtasks will be migrated from tidb_subtask_background
// to tidb_subtask_background_history. In the above situation,
// the built-in collector of Prometheus needs to delete the previously
// added metrics, which is quite troublesome.
// Therefore, a custom collector is used.
type collector struct {
	subtaskInfo atomic.Pointer[[]*proto.SubtaskBase]

	subtasks        *prometheus.Desc
	subtaskDuration *prometheus.Desc
}

func newCollector() *collector {
	return &collector{
		subtasks: prometheus.NewDesc(
			"tidb_disttask_subtasks",
			"Number of subtasks.",
			[]string{"task_type", "task_id", "status", "exec_id"}, nil,
		),
		subtaskDuration: prometheus.NewDesc(
			"tidb_disttask_subtask_duration",
			"Duration of subtasks in different states.",
			[]string{"task_type", "task_id", "status", "subtask_id", "exec_id"}, nil,
		),
	}
}

// Describe implements the prometheus.Collector interface.
func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.subtasks
	ch <- c.subtaskDuration
}

// Collect implements the prometheus.Collector interface.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	p := c.subtaskInfo.Load()
	if p == nil {
		return
	}
	subtasks := *p

	// taskID => execID => state => cnt
	subtaskCnt := make(map[int64]map[string]map[proto.SubtaskState]int)
	taskType := make(map[int64]proto.TaskType)
	for _, subtask := range subtasks {
		if _, ok := subtaskCnt[subtask.TaskID]; !ok {
			subtaskCnt[subtask.TaskID] = make(map[string]map[proto.SubtaskState]int)
		}
		if _, ok := subtaskCnt[subtask.TaskID][subtask.ExecID]; !ok {
			subtaskCnt[subtask.TaskID][subtask.ExecID] = make(map[proto.SubtaskState]int)
		}

		subtaskCnt[subtask.TaskID][subtask.ExecID][subtask.State]++
		taskType[subtask.TaskID] = subtask.Type

		c.setDistSubtaskDuration(ch, subtask)
	}
	for taskID, execIDMap := range subtaskCnt {
		for execID, stateMap := range execIDMap {
			for state, cnt := range stateMap {
				ch <- prometheus.MustNewConstMetric(c.subtasks, prometheus.GaugeValue,
					float64(cnt),
					taskType[taskID].String(),
					strconv.Itoa(int(taskID)),
					state.String(),
					execID,
				)
			}
		}
	}
}

func (c *collector) setDistSubtaskDuration(ch chan<- prometheus.Metric, subtask *proto.SubtaskBase) {
	switch subtask.State {
	case proto.SubtaskStatePending:
		ch <- prometheus.MustNewConstMetric(c.subtaskDuration, prometheus.GaugeValue,
			time.Since(subtask.CreateTime).Seconds(),
			subtask.Type.String(),
			strconv.Itoa(int(subtask.TaskID)),
			subtask.State.String(),
			strconv.Itoa(int(subtask.ID)),
			subtask.ExecID,
		)
	case proto.SubtaskStateRunning:
		ch <- prometheus.MustNewConstMetric(c.subtaskDuration, prometheus.GaugeValue,
			time.Since(subtask.StartTime).Seconds(),
			subtask.Type.String(),
			strconv.Itoa(int(subtask.TaskID)),
			subtask.State.String(),
			strconv.Itoa(int(subtask.ID)),
			subtask.ExecID,
		)
	}
}
