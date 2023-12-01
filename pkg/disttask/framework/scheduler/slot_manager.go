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

package scheduler

import (
	"context"

	"github.com/pingcap/tidb/pkg/disttask/framework/alloctor"
)

type slotManager struct {
	// taskID -> slotInfo
	schedulerSlotInfos map[int]slotInfo
	slotAlloctor       alloctor.Alloctor
}

type slotInfo struct {
	taskID    int
	priority  int
	slotCount int
}

func (sm *slotManager) init(ctx context.Context, taskTable TaskTable) error {
	// subtasks, err := taskTable.GetSubtasksByStepAndStates(ctx, proto.TaskStateRunning, proto.TaskStatePending)
	// if err != nil {
	// 	return err
	// }

	// for _, subtask := range subtasks {
	// 	sm.schedulerSlotInfos[int(subtask.TaskID)] = slotInfo{
	// 		taskID: int(subtask.TaskID),
	// 		// priority:  int(subtask.Priority),
	// 		slotCount: int(subtask.Concurrency),
	// 	}
	// }
	return nil
}
