// Copyright 2024 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// TransferSubtasks2HistoryWithSession transfer the selected subtasks into tidb_background_subtask_history table by taskID.
func (*TaskManager) TransferSubtasks2HistoryWithSession(ctx context.Context, se sessionctx.Context, taskID int64) error {
	exec := se.GetSQLExecutor()
	_, err := sqlexec.ExecSQL(ctx, exec, `insert into mysql.tidb_background_subtask_history select * from mysql.tidb_background_subtask where task_key = %?`, taskID)
	if err != nil {
		return err
	}
	// delete taskID subtask
	_, err = sqlexec.ExecSQL(ctx, exec, "delete from mysql.tidb_background_subtask where task_key = %?", taskID)
	return err
}

// TransferTasks2History transfer the selected tasks into tidb_global_task_history table by taskIDs.
func (mgr *TaskManager) TransferTasks2History(ctx context.Context, tasks []*proto.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	taskIDStrs := make([]string, 0, len(tasks))
	for _, task := range tasks {
		taskIDStrs = append(taskIDStrs, fmt.Sprintf("%d", task.ID))
	}
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		// sensitive data in meta might be redacted, need update first.
		exec := se.GetSQLExecutor()
		for _, t := range tasks {
			_, err := sqlexec.ExecSQL(ctx, exec, `
				update mysql.tidb_global_task
				set meta= %?, state_update_time = CURRENT_TIMESTAMP()
				where id = %?`, t.Meta, t.ID)
			if err != nil {
				return err
			}
		}
		_, err := sqlexec.ExecSQL(ctx, exec, `
			insert into mysql.tidb_global_task_history
			select * from mysql.tidb_global_task
			where id in(`+strings.Join(taskIDStrs, `, `)+`)`)
		if err != nil {
			return err
		}

		_, err = sqlexec.ExecSQL(ctx, exec, `
			delete from mysql.tidb_global_task
			where id in(`+strings.Join(taskIDStrs, `, `)+`)`)

		for _, t := range tasks {
			err = mgr.TransferSubtasks2HistoryWithSession(ctx, se, t.ID)
			if err != nil {
				return err
			}
		}
		return err
	})
}

// GCSubtasks deletes the history subtask which is older than the given days.
func (mgr *TaskManager) GCSubtasks(ctx context.Context) error {
	subtaskHistoryKeepSeconds := defaultSubtaskKeepDays * 24 * 60 * 60
	failpoint.Inject("subtaskHistoryKeepSeconds", func(val failpoint.Value) {
		if val, ok := val.(int); ok {
			subtaskHistoryKeepSeconds = val
		}
	})
	_, err := mgr.ExecuteSQLWithNewSession(
		ctx,
		fmt.Sprintf("DELETE FROM mysql.tidb_background_subtask_history WHERE state_update_time < UNIX_TIMESTAMP() - %d ;", subtaskHistoryKeepSeconds),
	)
	return err
}
