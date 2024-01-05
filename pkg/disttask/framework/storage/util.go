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

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// GetSubtasksFromHistoryForTest gets subtasks from history table for test.
func GetSubtasksFromHistoryForTest(ctx context.Context, stm *TaskManager) (int, error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
		"select * from mysql.tidb_background_subtask_history")
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// GetSubtasksFromHistoryByTaskIDForTest gets subtasks by taskID from history table for test.
func GetSubtasksFromHistoryByTaskIDForTest(ctx context.Context, stm *TaskManager, taskID int64) (int, error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
		`select `+subtaskColumns+` from mysql.tidb_background_subtask_history where task_key = %?`, taskID)
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// GetSubtasksByTaskIDForTest gets subtasks by taskID for test.
func GetSubtasksByTaskIDForTest(ctx context.Context, stm *TaskManager, taskID int64) ([]*proto.Subtask, error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
		`select `+subtaskColumns+` from mysql.tidb_background_subtask where task_key = %?`, taskID)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	subtasks := make([]*proto.Subtask, 0, len(rs))
	for _, r := range rs {
		subtasks = append(subtasks, row2SubTask(r))
	}
	return subtasks, nil
}

// GetTasksFromHistoryForTest gets tasks from history table for test.
func GetTasksFromHistoryForTest(ctx context.Context, stm *TaskManager) (int, error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
		"select * from mysql.tidb_global_task_history")
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// GetTaskEndTimeForTest gets task's endTime for test.
func GetTaskEndTimeForTest(ctx context.Context, stm *TaskManager, taskID int64) (time.Time, error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
		`select end_time 
		from mysql.tidb_global_task
	    where id = %?`, taskID)

	if err != nil {
		return time.Time{}, nil
	}
	if !rs[0].IsNull(0) {
		return rs[0].GetTime(0).GoTime(time.Local)
	}
	return time.Time{}, nil
}

// GetSubtaskEndTimeForTest gets subtask's endTime for test.
func GetSubtaskEndTimeForTest(ctx context.Context, stm *TaskManager, subtaskID int64) (time.Time, error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
		`select end_time 
		from mysql.tidb_background_subtask
	    where id = %?`, subtaskID)

	if err != nil {
		return time.Time{}, nil
	}
	if !rs[0].IsNull(0) {
		return rs[0].GetTime(0).GoTime(time.Local)
	}
	return time.Time{}, nil
}

// PrintSubtaskInfo log the subtask info by taskKey. Only used for UT.
func (stm *TaskManager) PrintSubtaskInfo(ctx context.Context, taskID int64) {
	rs, _ := stm.executeSQLWithNewSession(ctx,
		`select `+subtaskColumns+` from mysql.tidb_background_subtask_history where task_key = %?`, taskID)
	rs2, _ := stm.executeSQLWithNewSession(ctx,
		`select `+subtaskColumns+` from mysql.tidb_background_subtask where task_key = %?`, taskID)
	rs = append(rs, rs2...)

	for _, r := range rs {
		errBytes := r.GetBytes(13)
		var err error
		if len(errBytes) > 0 {
			stdErr := errors.Normalize("")
			err1 := stdErr.UnmarshalJSON(errBytes)
			if err1 != nil {
				err = err1
			} else {
				err = stdErr
			}
		}
		logutil.BgLogger().Info(fmt.Sprintf("subTask: %v\n", row2SubTask(r)), zap.Error(err))
	}
}
