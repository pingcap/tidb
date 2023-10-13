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

import "github.com/pingcap/tidb/pkg/disttask/framework/proto"

// GetSubtasksFromHistoryForTest gets subtasks from history table for test.
func GetSubtasksFromHistoryForTest(stm *TaskManager) (int, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx,
		"select * from mysql.tidb_background_subtask_history")
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// GetSubtasksFromHistoryByTaskIDForTest gets subtasks by taskID from history table for test.
func GetSubtasksFromHistoryByTaskIDForTest(stm *TaskManager, taskID int64) (int, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx,
		"select * from mysql.tidb_background_subtask_history where task_key = %?", taskID)
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// GetSubtasksByTaskIDForTest gets subtasks by taskID for test.
func GetSubtasksByTaskIDForTest(stm *TaskManager, taskID int64) ([]*proto.Subtask, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx,
		"select * from mysql.tidb_background_subtask where task_key = %?", taskID)
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
func GetTasksFromHistoryForTest(stm *TaskManager) (int, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx,
		"select * from mysql.tidb_global_task_history")
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}
