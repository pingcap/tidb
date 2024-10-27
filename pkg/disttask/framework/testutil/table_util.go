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

package testutil

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

// InitTableTest inits needed components for table_test.
// it disables disttask and mock cpu count to 8.
func InitTableTest(t *testing.T) (kv.Storage, *storage.TaskManager, context.Context) {
	store, pool := getResourcePool(t)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)")
	return store, getTaskManager(t, pool), ctx
}

// InitTableTestWithCancel inits needed components with context.CancelFunc for table_test.
func InitTableTestWithCancel(t *testing.T) (*storage.TaskManager, context.Context, context.CancelFunc) {
	_, pool := getResourcePool(t)
	ctx, cancel := context.WithCancel(context.Background())
	ctx = util.WithInternalSourceType(ctx, "table_test")
	return getTaskManager(t, pool), ctx, cancel
}

func getResourcePool(t *testing.T) (kv.Storage, *pools.ResourcePool) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)

	t.Cleanup(func() {
		pool.Close()
	})
	return store, pool
}

func getTaskManager(t *testing.T, pool *pools.ResourcePool) *storage.TaskManager {
	manager := storage.NewTaskManager(pool)
	storage.SetTaskManager(manager)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	return manager
}

// GetOneTask get a task from task table
func GetOneTask(ctx context.Context, mgr *storage.TaskManager) (task *proto.Task, err error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+storage.TaskColumns+" from mysql.tidb_global_task t where state = %? limit 1", proto.TaskStatePending)
	if err != nil {
		return task, err
	}

	if len(rs) == 0 {
		return nil, nil
	}

	return storage.Row2Task(rs[0]), nil
}

// GetSubtasksFromHistory gets subtasks from history table for test.
func GetSubtasksFromHistory(ctx context.Context, mgr *storage.TaskManager) (int, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		"select * from mysql.tidb_background_subtask_history")
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// GetSubtasksFromHistoryByTaskID gets subtasks by taskID from history table for test.
func GetSubtasksFromHistoryByTaskID(ctx context.Context, mgr *storage.TaskManager, taskID int64) (int, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select `+storage.SubtaskColumns+` from mysql.tidb_background_subtask_history where task_key = %?`, taskID)
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// GetSubtasksByTaskID gets subtasks by taskID for test.
func GetSubtasksByTaskID(ctx context.Context, mgr *storage.TaskManager, taskID int64) ([]*proto.Subtask, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select `+storage.SubtaskColumns+` from mysql.tidb_background_subtask where task_key = %?`, taskID)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	subtasks := make([]*proto.Subtask, 0, len(rs))
	for _, r := range rs {
		subtasks = append(subtasks, storage.Row2SubTask(r))
	}
	return subtasks, nil
}

// GetTasksFromHistory gets tasks from history table for test.
func GetTasksFromHistory(ctx context.Context, mgr *storage.TaskManager) (int, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		"select * from mysql.tidb_global_task_history")
	if err != nil {
		return 0, err
	}
	return len(rs), nil
}

// GetTaskEndTime gets task's endTime for test.
func GetTaskEndTime(ctx context.Context, mgr *storage.TaskManager, taskID int64) (time.Time, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
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

// GetSubtaskEndTime gets subtask's endTime for test.
func GetSubtaskEndTime(ctx context.Context, mgr *storage.TaskManager, subtaskID int64) (time.Time, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
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

// GetSubtaskNodes gets nodes that are running or have run the task for test.
func GetSubtaskNodes(ctx context.Context, mgr *storage.TaskManager, taskID int64) ([]string, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `
		select distinct(exec_id) from mysql.tidb_background_subtask where task_key=%?
		union
		select distinct(exec_id) from mysql.tidb_background_subtask_history where task_key=%?`, taskID, taskID)
	if err != nil {
		return nil, err
	}
	nodes := make([]string, 0, len(rs))
	for _, r := range rs {
		if !r.IsNull(0) {
			nodes = append(nodes, r.GetString(0))
		}
	}
	return nodes, nil
}

// UpdateSubtaskExecID updates the subtask's exec_id, used for testing now.
func UpdateSubtaskExecID(ctx context.Context, mgr *storage.TaskManager, tidbID string, subtaskID int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask
		 set exec_id = %?, state_update_time = unix_timestamp()
		 where id = %?`,
		tidbID, subtaskID)
	return err
}

// TransferSubTasks2History move subtasks from tidb_background_subtask to tidb_background_subtask_history.
func TransferSubTasks2History(ctx context.Context, mgr *storage.TaskManager, taskID int64) error {
	return mgr.WithNewSession(func(se sessionctx.Context) error {
		return mgr.TransferSubtasks2HistoryWithSession(ctx, se, taskID)
	})
}

// GetTasksFromHistoryInStates gets the tasks in history table in the states.
func GetTasksFromHistoryInStates(ctx context.Context, mgr *storage.TaskManager, states ...any) (task []*proto.Task, err error) {
	if len(states) == 0 {
		return task, nil
	}

	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+storage.TaskColumns+" from mysql.tidb_global_task_history t where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", states...)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		task = append(task, storage.Row2Task(r))
	}
	return task, nil
}

// DeleteSubtasksByTaskID deletes the subtask of the given task ID.
func DeleteSubtasksByTaskID(ctx context.Context, mgr *storage.TaskManager, taskID int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx, `delete from mysql.tidb_background_subtask
		where task_key = %?`, taskID)
	if err != nil {
		return err
	}

	return nil
}

// IsTaskCancelling checks whether the task state is cancelling.
func IsTaskCancelling(ctx context.Context, mgr *storage.TaskManager, taskID int64) (bool, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select 1 from mysql.tidb_global_task where id=%? and state = %?",
		taskID, proto.TaskStateCancelling,
	)

	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

// PrintSubtaskInfo log the subtask info by taskKey for test.
func PrintSubtaskInfo(ctx context.Context, mgr *storage.TaskManager, taskID int64) {
	rs, _ := mgr.ExecuteSQLWithNewSession(ctx,
		`select `+storage.SubtaskColumns+` from mysql.tidb_background_subtask_history where task_key = %?`, taskID)
	rs2, _ := mgr.ExecuteSQLWithNewSession(ctx,
		`select `+storage.SubtaskColumns+` from mysql.tidb_background_subtask where task_key = %?`, taskID)
	rs = append(rs, rs2...)

	for _, r := range rs {
		logutil.BgLogger().Info(fmt.Sprintf("subTask: %v\n", storage.Row2SubTask(r)))
	}
}
