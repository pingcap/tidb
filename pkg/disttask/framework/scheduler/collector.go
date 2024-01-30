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
	"context"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type collector struct {
	ctx context.Context
	Param

	subtasks        *prometheus.Desc
	subtaskDuration *prometheus.Desc
}

func newCollector(ctx context.Context, param Param) *collector {
	return &collector{
		ctx:   ctx,
		Param: param,

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

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.subtasks
	ch <- c.subtaskDuration
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	subtasks, err := c.taskMgr.GetAllSubtasks(c.ctx)
	if err != nil {
		logutil.BgLogger().Warn("get all subtasks failed", zap.Error(err))
		return
	}
	allNodes, err := c.taskMgr.GetAllNodes(c.ctx)
	if err != nil {
		logutil.BgLogger().Warn("get all nodes failed", zap.Error(err))
		return
	}
	// taskID => execID => state => cnt
	subtaskCnt := make(map[int64]map[string]map[proto.SubtaskState]int)
	taskType := make(map[int64]proto.TaskType)
	for _, subtask := range subtasks {
		if _, ok := subtaskCnt[subtask.TaskID]; !ok {
			subtaskCnt[subtask.TaskID] = make(map[string]map[proto.SubtaskState]int, len(allNodes))
			for _, node := range allNodes {
				subtaskCnt[subtask.TaskID][node.ID] = make(map[proto.SubtaskState]int, len(proto.AllSubtaskStates))
				for _, state := range proto.AllSubtaskStates {
					subtaskCnt[subtask.TaskID][node.ID][state] = 0
				}
			}
		}
		if _, ok := subtaskCnt[subtask.TaskID][subtask.ExecID]; !ok {
			logutil.BgLogger().Warn("the execID of subtask is not found in meta", zap.Stringer("subtask", subtask))
			return
		}
		subtaskCnt[subtask.TaskID][subtask.ExecID][subtask.State]++
		taskType[subtask.TaskID] = subtask.Type

		c.setDistSubtaskDuration(ch, subtask)
	}
	for taskID, execIDMap := range subtaskCnt {
		for execID, stateMap := range execIDMap {
			for state, cnt := range stateMap {
				if cnt == 0 {
					continue
				}
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

func (c *collector) setDistSubtaskDuration(ch chan<- prometheus.Metric, subtask *proto.Subtask) {
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
