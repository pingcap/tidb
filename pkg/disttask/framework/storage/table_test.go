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

package storage_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func GetTaskManager(t *testing.T, pool *pools.ResourcePool) *storage.TaskManager {
	manager := storage.NewTaskManager(pool)
	storage.SetTaskManager(manager)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	return manager
}

func checkTaskStateStep(t *testing.T, task *proto.Task, state proto.TaskState, step proto.Step) {
	require.Equal(t, state, task.State)
	require.Equal(t, step, task.Step)
}

func TestTaskTable(t *testing.T) {
	gm, ctx := testutil.InitTableTest(t)

	_, err := gm.CreateTask(ctx, "key1", "test", 999, []byte("test"))
	require.ErrorContains(t, err, "task concurrency(999) larger than cpu count")

	timeBeforeCreate := time.Unix(time.Now().Unix(), 0)
	id, err := gm.CreateTask(ctx, "key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := gm.GetOneTask(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), task.ID)
	require.Equal(t, "key1", task.Key)
	require.Equal(t, proto.TaskType("test"), task.Type)
	require.Equal(t, proto.TaskStatePending, task.State)
	require.Equal(t, proto.NormalPriority, task.Priority)
	require.Equal(t, 4, task.Concurrency)
	require.Equal(t, proto.StepInit, task.Step)
	require.Equal(t, []byte("test"), task.Meta)
	require.GreaterOrEqual(t, task.CreateTime, timeBeforeCreate)
	require.Zero(t, task.StartTime)
	require.Zero(t, task.StateUpdateTime)
	require.Nil(t, task.Error)

	task2, err := gm.GetTaskByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, task, task2)

	task3, err := gm.GetTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Len(t, task3, 1)
	require.Equal(t, task, task3[0])

	task4, err := gm.GetTasksInStates(ctx, proto.TaskStatePending, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task4, 1)
	require.Equal(t, task, task4[0])
	require.GreaterOrEqual(t, task4[0].StateUpdateTime, task.StateUpdateTime)

	prevState := task.State
	task.State = proto.TaskStateRunning
	retryable, err := gm.UpdateTaskAndAddSubTasks(ctx, task, nil, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task5, err := gm.GetTasksInStates(ctx, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task.State, task5[0].State)

	task6, err := gm.GetTaskByKey(ctx, "key1")
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task.State, task6.State)

	// test cannot insert task with dup key
	_, err = gm.CreateTask(ctx, "key1", "test2", 4, []byte("test2"))
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'key1' for key 'tidb_global_task.task_key'")

	// test cancel task
	id, err = gm.CreateTask(ctx, "key2", "test", 4, []byte("test"))
	require.NoError(t, err)

	cancelling, err := gm.IsTaskCancelling(ctx, id)
	require.NoError(t, err)
	require.False(t, cancelling)

	require.NoError(t, gm.CancelTask(ctx, id))
	cancelling, err = gm.IsTaskCancelling(ctx, id)
	require.NoError(t, err)
	require.True(t, cancelling)

	id, err = gm.CreateTask(ctx, "key-fail", "test2", 4, []byte("test2"))
	require.NoError(t, err)
	// state not right, update nothing
	require.NoError(t, gm.FailTask(ctx, id, proto.TaskStateRunning, errors.New("test error")))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePending, task.State)
	require.Nil(t, task.Error)
	require.NoError(t, gm.FailTask(ctx, id, proto.TaskStatePending, errors.New("test error")))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateFailed, task.State)
	require.ErrorContains(t, task.Error, "test error")

	// succeed a pending task, no effect
	id, err = gm.CreateTask(ctx, "key-success", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.NoError(t, gm.SucceedTask(ctx, id))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStatePending, proto.StepInit)
	// succeed a running task
	require.NoError(t, gm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, nil))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateRunning, proto.StepOne)
	startTime := time.Unix(time.Now().Unix(), 0)
	require.NoError(t, gm.SucceedTask(ctx, id))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateSucceed, proto.StepDone)
	require.GreaterOrEqual(t, task.StateUpdateTime, startTime)
}

func checkAfterSwitchStep(t *testing.T, startTime time.Time, task *proto.Task, subtasks []*proto.Subtask, step proto.Step) {
	tm, err := storage.GetTaskManager()
	require.NoError(t, err)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	checkTaskStateStep(t, task, proto.TaskStateRunning, step)
	require.GreaterOrEqual(t, task.StartTime, startTime)
	require.GreaterOrEqual(t, task.StateUpdateTime, startTime)
	gotSubtasks, err := tm.GetSubtasksByStepAndState(ctx, task.ID, task.Step, proto.TaskStatePending)
	require.NoError(t, err)
	require.Len(t, gotSubtasks, len(subtasks))
	sort.Slice(gotSubtasks, func(i, j int) bool {
		return gotSubtasks[i].Ordinal < gotSubtasks[j].Ordinal
	})
	for i := 0; i < len(gotSubtasks); i++ {
		subtask := gotSubtasks[i]
		require.Equal(t, []byte(fmt.Sprintf("%d", i)), subtask.Meta)
		require.Equal(t, i+1, subtask.Ordinal)
		require.Equal(t, 11, subtask.Concurrency)
		require.Equal(t, ":4000", subtask.ExecID)
		require.Equal(t, proto.TaskTypeExample, subtask.Type)
		require.GreaterOrEqual(t, subtask.CreateTime, startTime)
	}
}

func TestSwitchTaskStep(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	tm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	taskID, err := tm.CreateTask(ctx, "key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	task, err := tm.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStatePending, proto.StepInit)
	subtasksStepOne := make([]*proto.Subtask, 3)
	for i := 0; i < len(subtasksStepOne); i++ {
		subtasksStepOne[i] = proto.NewSubtask(proto.StepOne, taskID, proto.TaskTypeExample,
			":4000", 11, []byte(fmt.Sprintf("%d", i)), i+1)
	}
	startTime := time.Unix(time.Now().Unix(), 0)
	task.Meta = []byte("changed meta")
	require.NoError(t, tm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, subtasksStepOne))
	task, err = tm.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, []byte("changed meta"), task.Meta)
	checkAfterSwitchStep(t, startTime, task, subtasksStepOne, proto.StepOne)
	// switch step again, no effect
	require.NoError(t, tm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, subtasksStepOne))
	task, err = tm.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	checkAfterSwitchStep(t, startTime, task, subtasksStepOne, proto.StepOne)
	// switch step to step two
	time.Sleep(time.Second)
	taskStartTime := task.StartTime
	subtasksStepTwo := make([]*proto.Subtask, 3)
	for i := 0; i < len(subtasksStepTwo); i++ {
		subtasksStepTwo[i] = proto.NewSubtask(proto.StepTwo, taskID, proto.TaskTypeExample,
			":4000", 11, []byte(fmt.Sprintf("%d", i)), i+1)
	}
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar(variable.TiDBMemQuotaQuery, "1024"))
	require.NoError(t, tm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepTwo, subtasksStepTwo))
	value, ok := tk.Session().GetSessionVars().GetSystemVar(variable.TiDBMemQuotaQuery)
	require.True(t, ok)
	require.Equal(t, "1024", value)
	task, err = tm.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	// start time should not change
	require.Equal(t, taskStartTime, task.StartTime)
	checkAfterSwitchStep(t, startTime, task, subtasksStepTwo, proto.StepTwo)
}

func TestSwitchTaskStepInBatch(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	tm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	// normal flow
	prepare := func(taskKey string) (*proto.Task, []*proto.Subtask) {
		taskID, err := tm.CreateTask(ctx, taskKey, "test", 4, []byte("test"))
		require.NoError(t, err)
		task, err := tm.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		checkTaskStateStep(t, task, proto.TaskStatePending, proto.StepInit)
		subtasks := make([]*proto.Subtask, 3)
		for i := 0; i < len(subtasks); i++ {
			subtasks[i] = proto.NewSubtask(proto.StepOne, taskID, proto.TaskTypeExample,
				":4000", 11, []byte(fmt.Sprintf("%d", i)), i+1)
		}
		return task, subtasks
	}
	startTime := time.Unix(time.Now().Unix(), 0)
	task1, subtasks1 := prepare("key1")
	require.NoError(t, tm.SwitchTaskStepInBatch(ctx, task1, proto.TaskStateRunning, proto.StepOne, subtasks1))
	task1, err := tm.GetTaskByID(ctx, task1.ID)
	require.NoError(t, err)
	checkAfterSwitchStep(t, startTime, task1, subtasks1, proto.StepOne)

	// mock another dispatcher inserted some subtasks
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/waitBeforeInsertSubtasks", `1*return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/waitBeforeInsertSubtasks"))
	})
	task2, subtasks2 := prepare("key2")
	go func() {
		storage.TestChannel <- struct{}{}
		tk2 := testkit.NewTestKit(t, store)
		subtask := subtasks2[0]
		_, err = storage.ExecSQL(ctx, tk2.Session(), `
			insert into mysql.tidb_background_subtask(
				step, task_key, exec_id, meta, state, type, concurrency, ordinal, create_time, checkpoint, summary)
			values (%?, %?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', '{}')`,
			subtask.Step, subtask.TaskID, subtask.ExecID, subtask.Meta,
			proto.TaskStatePending, proto.Type2Int(subtask.Type), subtask.Concurrency, subtask.Ordinal)
		require.NoError(t, err)
		storage.TestChannel <- struct{}{}
	}()
	err = tm.SwitchTaskStepInBatch(ctx, task2, proto.TaskStateRunning, proto.StepOne, subtasks2)
	require.ErrorIs(t, err, kv.ErrKeyExists)
	task2, err = tm.GetTaskByID(ctx, task2.ID)
	require.NoError(t, err)
	checkTaskStateStep(t, task2, proto.TaskStatePending, proto.StepInit)
	gotSubtasks, err := tm.GetSubtasksByStepAndState(ctx, task2.ID, proto.StepOne, proto.TaskStatePending)
	require.NoError(t, err)
	require.Len(t, gotSubtasks, 1)
	// run again, should success
	require.NoError(t, tm.SwitchTaskStepInBatch(ctx, task2, proto.TaskStateRunning, proto.StepOne, subtasks2))
	task2, err = tm.GetTaskByID(ctx, task2.ID)
	require.NoError(t, err)
	checkAfterSwitchStep(t, startTime, task2, subtasks2, proto.StepOne)

	// mock subtasks unstable
	task3, subtasks3 := prepare("key3")
	for i := 0; i < 2; i++ {
		subtask := subtasks3[i]
		_, err = storage.ExecSQL(ctx, tk.Session(), `
			insert into mysql.tidb_background_subtask(
				step, task_key, exec_id, meta, state, type, concurrency, ordinal, create_time, checkpoint, summary)
			values (%?, %?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', '{}')`,
			subtask.Step, subtask.TaskID, subtask.ExecID, subtask.Meta,
			proto.TaskStatePending, proto.Type2Int(subtask.Type), subtask.Concurrency, subtask.Ordinal)
		require.NoError(t, err)
	}
	err = tm.SwitchTaskStepInBatch(ctx, task3, proto.TaskStateRunning, proto.StepOne, subtasks3[:1])
	require.ErrorIs(t, err, storage.ErrUnstableSubtasks)
	require.ErrorContains(t, err, "expected 1, got 2")
}

func TestGetTopUnfinishedTasks(t *testing.T) {
	gm, ctx := testutil.InitTableTest(t)

	taskStates := []proto.TaskState{
		proto.TaskStateSucceed,
		proto.TaskStatePending,
		proto.TaskStateRunning,
		proto.TaskStateReverting,
		proto.TaskStateCancelling,
		proto.TaskStatePausing,
		proto.TaskStateResuming,
		proto.TaskStateFailed,
		proto.TaskStatePending,
		proto.TaskStatePending,
		proto.TaskStatePending,
		proto.TaskStatePending,
	}
	for i, state := range taskStates {
		taskKey := fmt.Sprintf("key/%d", i)
		_, err := gm.CreateTask(ctx, taskKey, "test", 4, []byte("test"))
		require.NoError(t, err)
		require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
			_, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, `
				update mysql.tidb_global_task set state = %? where task_key = %?`,
				state, taskKey)
			return err
		}))
	}
	// adjust task order
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		_, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, `
				update mysql.tidb_global_task set create_time = current_timestamp`)
		return err
	}))
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		_, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, `
				update mysql.tidb_global_task
				set create_time = timestampadd(minute, -10, current_timestamp)
				where task_key = 'key/5'`)
		return err
	}))
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		_, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, `
				update mysql.tidb_global_task set priority = 100 where task_key = 'key/6'`)
		return err
	}))
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		rs, err := storage.ExecSQL(ctx, se, `
				select count(1) from mysql.tidb_global_task`)
		require.Len(t, rs, 1)
		require.Equal(t, int64(12), rs[0].GetInt64(0))
		return err
	}))
	tasks, err := gm.GetTopUnfinishedTasks(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 8)
	taskKeys := make([]string, 0, len(tasks))
	for _, task := range tasks {
		taskKeys = append(taskKeys, task.Key)
		// not filled
		require.Empty(t, task.Meta)
	}
	require.Equal(t, []string{"key/6", "key/5", "key/1", "key/2", "key/3", "key/4", "key/8", "key/9"}, taskKeys)
}

func TestGetUsedSlotsOnNodes(t *testing.T) {
	sm, ctx := testutil.InitTableTest(t)

	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb-1", []byte(""), proto.TaskStateRunning, "test", 12)
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb-2", []byte(""), proto.TaskStatePending, "test", 12)
	testutil.InsertSubtask(t, sm, 2, proto.StepOne, "tidb-2", []byte(""), proto.TaskStatePending, "test", 8)
	testutil.InsertSubtask(t, sm, 3, proto.StepOne, "tidb-3", []byte(""), proto.TaskStatePending, "test", 8)
	testutil.InsertSubtask(t, sm, 4, proto.StepOne, "tidb-3", []byte(""), proto.TaskStateFailed, "test", 8)
	slotsOnNodes, err := sm.GetUsedSlotsOnNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]int{
		"tidb-1": 12,
		"tidb-2": 20,
		"tidb-3": 8,
	}, slotsOnNodes)
}

func TestSubTaskTable(t *testing.T) {
	sm, ctx := testutil.InitTableTest(t)
	timeBeforeCreate := time.Unix(time.Now().Unix(), 0)
	id, err := sm.CreateTask(ctx, "key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	_, err = sm.UpdateTaskAndAddSubTasks(
		ctx,
		&proto.Task{
			ID:    1,
			State: proto.TaskStateRunning,
		},
		[]*proto.Subtask{
			{
				Step:        proto.StepInit,
				Type:        proto.TaskTypeExample,
				Concurrency: 11,
				ExecID:      "tidb1",
				Meta:        []byte("test"),
			},
		}, proto.TaskStatePending,
	)
	require.NoError(t, err)

	nilTask, err := sm.GetFirstSubtaskInStates(ctx, "tidb2", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Nil(t, nilTask)

	subtask, err := sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, proto.StepInit, subtask.Step)
	require.Equal(t, int64(1), subtask.TaskID)
	require.Equal(t, proto.TaskTypeExample, subtask.Type)
	require.Equal(t, int64(1), subtask.TaskID)
	require.Equal(t, proto.TaskStatePending, subtask.State)
	require.Equal(t, "tidb1", subtask.ExecID)
	require.Equal(t, []byte("test"), subtask.Meta)
	require.Equal(t, 11, subtask.Concurrency)
	require.GreaterOrEqual(t, subtask.CreateTime, timeBeforeCreate)
	require.Zero(t, subtask.StartTime)
	require.Zero(t, subtask.UpdateTime)
	require.Equal(t, "{}", subtask.Summary)

	subtask2, err := sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending, proto.TaskStateReverted)
	require.NoError(t, err)
	require.Equal(t, subtask, subtask2)

	ids, err := sm.GetTaskExecutorIDsByTaskID(ctx, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, "tidb1", ids[0])

	ids, err = sm.GetTaskExecutorIDsByTaskID(ctx, 3)
	require.NoError(t, err)
	require.Len(t, ids, 0)

	cnt, err := sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStatePending, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	ok, err := sm.HasSubtasksInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.True(t, ok)

	ts := time.Now()
	time.Sleep(time.Second)
	require.NoError(t, sm.StartSubtask(ctx, 1))

	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Nil(t, subtask)

	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Equal(t, proto.TaskTypeExample, subtask.Type)
	require.Equal(t, int64(1), subtask.TaskID)
	require.Equal(t, proto.TaskStateRunning, subtask.State)
	require.Equal(t, "tidb1", subtask.ExecID)
	require.Equal(t, []byte("test"), subtask.Meta)
	require.GreaterOrEqual(t, subtask.StartTime, ts)
	require.GreaterOrEqual(t, subtask.UpdateTime, ts)

	// check update time after state change to cancel
	time.Sleep(time.Second)
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", 1, proto.TaskStateCancelling, nil))
	subtask2, err = sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStateCancelling)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateCancelling, subtask2.State)
	require.Greater(t, subtask2.UpdateTime, subtask.UpdateTime)

	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)

	ok, err = sm.HasSubtasksInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.False(t, ok)

	err = sm.DeleteSubtasksByTaskID(ctx, 1)
	require.NoError(t, err)

	ok, err = sm.HasSubtasksInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending, proto.TaskStateRunning)
	require.NoError(t, err)
	require.False(t, ok)

	testutil.CreateSubTask(t, sm, 2, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, true)

	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 2, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	subtasks, err := sm.GetSubtasksByStepAndState(ctx, 2, proto.StepInit, proto.TaskStateSucceed)
	require.NoError(t, err)
	require.Len(t, subtasks, 0)

	err = sm.FinishSubtask(ctx, "tidb1", 2, []byte{})
	require.NoError(t, err)

	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 2, proto.StepInit, proto.TaskStateSucceed)
	require.NoError(t, err)
	require.Len(t, subtasks, 1)

	rowCount, err := sm.GetSubtaskRowCount(ctx, 2, proto.StepInit)
	require.NoError(t, err)
	require.Equal(t, int64(0), rowCount)
	err = sm.UpdateSubtaskRowCount(ctx, 2, 100)
	require.NoError(t, err)
	rowCount, err = sm.GetSubtaskRowCount(ctx, 2, proto.StepInit)
	require.NoError(t, err)
	require.Equal(t, int64(100), rowCount)

	// test UpdateErrorToSubtask do update start/update time
	testutil.CreateSubTask(t, sm, 3, proto.StepInit, "for_test", []byte("test"), proto.TaskTypeExample, 11, false)
	require.NoError(t, sm.UpdateErrorToSubtask(ctx, "for_test", 3, errors.New("fail")))
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "for_test", 3, proto.StepInit, proto.TaskStateFailed)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateFailed, subtask.State)
	require.Greater(t, subtask.StartTime, ts)
	require.Greater(t, subtask.UpdateTime, ts)

	// test FinishSubtask do update update time
	testutil.CreateSubTask(t, sm, 4, proto.StepInit, "for_test1", []byte("test"), proto.TaskTypeExample, 11, false)
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "for_test1", 4, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.NoError(t, sm.StartSubtask(ctx, subtask.ID))
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "for_test1", 4, proto.StepInit, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Greater(t, subtask.StartTime, ts)
	require.Greater(t, subtask.UpdateTime, ts)
	time.Sleep(time.Second)
	require.NoError(t, sm.FinishSubtask(ctx, "for_test1", subtask.ID, []byte{}))
	subtask2, err = sm.GetFirstSubtaskInStates(ctx, "for_test1", 4, proto.StepInit, proto.TaskStateSucceed)
	require.NoError(t, err)
	require.Equal(t, subtask2.StartTime, subtask.StartTime)
	require.Greater(t, subtask2.UpdateTime, subtask.UpdateTime)

	// test UpdateFailedTaskExecutorIDs and IsTaskExecutorCanceled
	canceled, err := sm.IsTaskExecutorCanceled(ctx, "for_test999", 4)
	require.NoError(t, err)
	require.True(t, canceled)
	canceled, err = sm.IsTaskExecutorCanceled(ctx, "for_test1", 4)
	require.NoError(t, err)
	require.False(t, canceled)
	canceled, err = sm.IsTaskExecutorCanceled(ctx, "for_test2", 4)
	require.NoError(t, err)
	require.True(t, canceled)

	// test UpdateSubtasksExecIDs
	// 1. update one subtask
	testutil.CreateSubTask(t, sm, 5, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, false)
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	subtasks[0].ExecID = "tidb2"
	require.NoError(t, sm.UpdateSubtasksExecIDs(ctx, 5, subtasks))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb2", subtasks[0].ExecID)
	// 2. update 2 subtasks
	testutil.CreateSubTask(t, sm, 5, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, false)
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	subtasks[0].ExecID = "tidb3"
	require.NoError(t, sm.UpdateSubtasksExecIDs(ctx, 5, subtasks))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb3", subtasks[0].ExecID)
	require.Equal(t, "tidb1", subtasks[1].ExecID)
	// update fail
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", subtasks[0].ID, proto.TaskStateRunning, nil))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb3", subtasks[0].ExecID)
	subtasks[0].ExecID = "tidb2"
	// update success
	require.NoError(t, sm.UpdateSubtasksExecIDs(ctx, 5, subtasks))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb2", subtasks[0].ExecID)

	// test GetSubtasksByExecIdsAndStepAndState
	testutil.CreateSubTask(t, sm, 6, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, false)
	testutil.CreateSubTask(t, sm, 6, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, false)
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 6, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", subtasks[0].ID, proto.TaskStateRunning, nil))
	subtasks, err = sm.GetSubtasksByExecIdsAndStepAndState(ctx, []string{"tidb1"}, 6, proto.StepInit, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Equal(t, 1, len(subtasks))
	subtasks, err = sm.GetSubtasksByExecIdsAndStepAndState(ctx, []string{"tidb1"}, 6, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 1, len(subtasks))
	testutil.CreateSubTask(t, sm, 6, proto.StepInit, "tidb2", []byte("test"), proto.TaskTypeExample, 11, false)
	subtasks, err = sm.GetSubtasksByExecIdsAndStepAndState(ctx, []string{"tidb1", "tidb2"}, 6, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 2, len(subtasks))
	subtasks, err = sm.GetSubtasksByExecIdsAndStepAndState(ctx, []string{"tidb1"}, 6, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 1, len(subtasks))
}

func TestBothTaskAndSubTaskTable(t *testing.T) {
	sm, ctx := testutil.InitTableTest(t)
	id, err := sm.CreateTask(ctx, "key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := sm.GetOneTask(ctx)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePending, task.State)

	// isSubTaskRevert: false
	prevState := task.State
	task.State = proto.TaskStateRunning
	subTasks := []*proto.Subtask{
		{
			Step:   proto.StepInit,
			Type:   proto.TaskTypeExample,
			ExecID: "instance1",
			Meta:   []byte("m1"),
		},
		{
			Step:   proto.StepInit,
			Type:   proto.TaskTypeExample,
			ExecID: "instance2",
			Meta:   []byte("m2"),
		},
	}
	retryable, err := sm.UpdateTaskAndAddSubTasks(ctx, task, subTasks, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task, err = sm.GetTaskByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateRunning, task.State)

	subtask1, err := sm.GetFirstSubtaskInStates(ctx, "instance1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(1), subtask1.ID)
	require.Equal(t, proto.TaskTypeExample, subtask1.Type)
	require.Equal(t, []byte("m1"), subtask1.Meta)

	subtask2, err := sm.GetFirstSubtaskInStates(ctx, "instance2", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(2), subtask2.ID)
	require.Equal(t, proto.TaskTypeExample, subtask2.Type)
	require.Equal(t, []byte("m2"), subtask2.Meta)

	cnt, err := sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt)

	// isSubTaskRevert: true
	prevState = task.State
	task.State = proto.TaskStateReverting
	subTasks = []*proto.Subtask{
		{
			Step:   proto.StepInit,
			Type:   proto.TaskTypeExample,
			ExecID: "instance3",
			Meta:   []byte("m3"),
		},
		{
			Step:   proto.StepInit,
			Type:   proto.TaskTypeExample,
			ExecID: "instance4",
			Meta:   []byte("m4"),
		},
	}
	retryable, err = sm.UpdateTaskAndAddSubTasks(ctx, task, subTasks, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task, err = sm.GetTaskByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateReverting, task.State)

	subtask1, err = sm.GetFirstSubtaskInStates(ctx, "instance3", 1, proto.StepInit, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(3), subtask1.ID)
	require.Equal(t, proto.TaskTypeExample, subtask1.Type)
	require.Equal(t, []byte("m3"), subtask1.Meta)

	subtask2, err = sm.GetFirstSubtaskInStates(ctx, "instance4", 1, proto.StepInit, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(4), subtask2.ID)
	require.Equal(t, proto.TaskTypeExample, subtask2.Type)
	require.Equal(t, []byte("m4"), subtask2.Meta)

	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt)

	// test transactional
	require.NoError(t, sm.DeleteSubtasksByTaskID(ctx, 1))
	failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/MockUpdateTaskErr", "1*return(true)")
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/MockUpdateTaskErr"))
	}()
	prevState = task.State
	task.State = proto.TaskStateFailed
	retryable, err = sm.UpdateTaskAndAddSubTasks(ctx, task, subTasks, prevState)
	require.EqualError(t, err, "updateTaskErr")
	require.Equal(t, true, retryable)

	task, err = sm.GetTaskByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateReverting, task.State)

	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)
}

func TestDistFrameworkMeta(t *testing.T) {
	// to avoid inserted nodes be cleaned by scheduler
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
	}()
	sm, ctx := testutil.InitTableTest(t)

	require.NoError(t, sm.StartManager(ctx, ":4000", "background"))
	require.NoError(t, sm.StartManager(ctx, ":4001", ""))
	require.NoError(t, sm.StartManager(ctx, ":4002", "background"))
	// won't be replaced by below one
	require.NoError(t, sm.StartManager(ctx, ":4002", ""))
	require.NoError(t, sm.StartManager(ctx, ":4003", "background"))

	nodes, err := sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{":4000", ":4001", ":4002", ":4003"}, nodes)

	require.NoError(t, sm.CleanUpMeta(ctx, []string{":4000"}))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{":4002", ":4003"}, nodes)

	require.NoError(t, sm.CleanUpMeta(ctx, []string{":4003"}))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{":4002"}, nodes)

	require.NoError(t, sm.CleanUpMeta(ctx, []string{":4002"}))
	nodes, err = sm.GetManagedNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{":4001"}, nodes)
}

func TestSubtaskHistoryTable(t *testing.T) {
	sm, ctx := testutil.InitTableTest(t)

	const (
		taskID       = 1
		taskID2      = 2
		subTask1     = 1
		subTask2     = 2
		subTask3     = 3
		subTask4     = 4 // taskID2
		tidb1        = "tidb1"
		tidb2        = "tidb2"
		tidb3        = "tidb3"
		meta         = "test"
		finishedMeta = "finished"
	)

	testutil.CreateSubTask(t, sm, taskID, proto.StepInit, tidb1, []byte(meta), proto.TaskTypeExample, 11, false)
	require.NoError(t, sm.FinishSubtask(ctx, tidb1, subTask1, []byte(finishedMeta)))
	testutil.CreateSubTask(t, sm, taskID, proto.StepInit, tidb2, []byte(meta), proto.TaskTypeExample, 11, false)
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, tidb2, subTask2, proto.TaskStateCanceled, nil))
	testutil.CreateSubTask(t, sm, taskID, proto.StepInit, tidb3, []byte(meta), proto.TaskTypeExample, 11, false)
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, tidb3, subTask3, proto.TaskStateFailed, nil))

	subTasks, err := storage.GetSubtasksByTaskIDForTest(ctx, sm, taskID)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)
	historySubTasksCnt, err := storage.GetSubtasksFromHistoryForTest(ctx, sm)
	require.NoError(t, err)
	require.Equal(t, 0, historySubTasksCnt)
	subTasks, err = sm.GetSubtasksForImportInto(ctx, taskID, proto.StepInit)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)

	// test TransferSubTasks2History
	require.NoError(t, sm.TransferSubTasks2History(ctx, taskID))

	subTasks, err = storage.GetSubtasksByTaskIDForTest(ctx, sm, taskID)
	require.NoError(t, err)
	require.Len(t, subTasks, 0)
	historySubTasksCnt, err = storage.GetSubtasksFromHistoryForTest(ctx, sm)
	require.NoError(t, err)
	require.Equal(t, 3, historySubTasksCnt)
	subTasks, err = sm.GetSubtasksForImportInto(ctx, taskID, proto.StepInit)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)

	// test GC history table.
	failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds", "return(1)")
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds"))
	}()
	time.Sleep(2 * time.Second)

	testutil.CreateSubTask(t, sm, taskID2, proto.StepInit, tidb1, []byte(meta), proto.TaskTypeExample, 11, false)
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, tidb1, subTask4, proto.TaskStateFailed, nil))
	require.NoError(t, sm.TransferSubTasks2History(ctx, taskID2))

	require.NoError(t, sm.GCSubtasks(ctx))

	historySubTasksCnt, err = storage.GetSubtasksFromHistoryForTest(ctx, sm)
	require.NoError(t, err)
	require.Equal(t, 1, historySubTasksCnt)
}

func TestTaskHistoryTable(t *testing.T) {
	gm, ctx := testutil.InitTableTest(t)

	_, err := gm.CreateTask(ctx, "1", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	taskID, err := gm.CreateTask(ctx, "2", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)

	tasks, err := gm.GetTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 2, len(tasks))

	require.NoError(t, gm.TransferTasks2History(ctx, tasks))

	tasks, err = gm.GetTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 0, len(tasks))
	num, err := storage.GetTasksFromHistoryForTest(ctx, gm)
	require.NoError(t, err)
	require.Equal(t, 2, num)

	task, err := gm.GetTaskByIDWithHistory(ctx, taskID)
	require.NoError(t, err)
	require.NotNil(t, task)

	task, err = gm.GetTaskByKeyWithHistory(ctx, "1")
	require.NoError(t, err)
	require.NotNil(t, task)

	// task with fail transfer
	_, err = gm.CreateTask(ctx, "3", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	tasks, err = gm.GetTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	tasks[0].Error = errors.New("mock err")
	require.NoError(t, gm.TransferTasks2History(ctx, tasks))
	num, err = storage.GetTasksFromHistoryForTest(ctx, gm)
	require.NoError(t, err)
	require.Equal(t, 3, num)
}

func TestPauseAndResume(t *testing.T) {
	sm, ctx := testutil.InitTableTest(t)

	testutil.CreateSubTask(t, sm, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, false)
	testutil.CreateSubTask(t, sm, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, false)
	testutil.CreateSubTask(t, sm, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, false)
	// 1.1 pause all subtasks.
	require.NoError(t, sm.PauseSubtasks(ctx, "tidb1", 1))
	cnt, err := sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStatePaused)
	require.NoError(t, err)
	require.Equal(t, int64(3), cnt)
	// 1.2 resume all subtasks.
	require.NoError(t, sm.ResumeSubtasks(ctx, 1))
	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(3), cnt)

	// 2.1 pause 2 subtasks.
	sm.UpdateSubtaskStateAndError(ctx, "tidb1", 1, proto.TaskStateSucceed, nil)
	require.NoError(t, sm.PauseSubtasks(ctx, "tidb1", 1))
	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStatePaused)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt)
	// 2.2 resume 2 subtasks.
	require.NoError(t, sm.ResumeSubtasks(ctx, 1))
	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt)
}

func TestCancelAndExecIdChanged(t *testing.T) {
	sm, ctx, cancel := testutil.InitTableTestWithCancel(t)

	testutil.CreateSubTask(t, sm, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11, false)
	subtask, err := sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	// 1. cancel the ctx, then update subtask state.
	cancel()
	require.ErrorIs(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", subtask.ID, proto.TaskStateFailed, nil), context.Canceled)
	ctx = context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending)
	// task state not changed
	require.NoError(t, err)
	require.Equal(t, subtask.State, proto.TaskStatePending)

	// 2. change the exec_id
	// exec_id changed
	require.NoError(t, sm.UpdateSubtaskExecID(ctx, "tidb2", subtask.ID))
	// exec_id in memory unchanged, call UpdateSubtaskStateAndError.
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, subtask.ExecID, subtask.ID, proto.TaskStateFailed, nil))
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb2", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	// state unchanged
	require.NotNil(t, subtask)
}
