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
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func checkTaskStateStep(t *testing.T, task *proto.Task, state proto.TaskState, step proto.Step) {
	require.Equal(t, state, task.State)
	require.Equal(t, step, task.Step)
}

func TestTaskTable(t *testing.T) {
	_, gm, ctx := testutil.InitTableTest(t)

	require.NoError(t, gm.InitMeta(ctx, ":4000", ""))

	_, err := gm.CreateTask(ctx, "key1", "test", 999, "", []byte("test"))
	require.ErrorContains(t, err, "task concurrency(999) larger than cpu count")

	timeBeforeCreate := time.Unix(time.Now().Unix(), 0)
	id, err := gm.CreateTask(ctx, "key1", "test", 4, "", []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := testutil.GetOneTask(ctx, gm)
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

	err = gm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, nil)
	require.NoError(t, err)

	task.State = proto.TaskStateRunning
	task5, err := gm.GetTasksInStates(ctx, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task.State, task5[0].State)

	task6, err := gm.GetTaskByKey(ctx, "key1")
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task.State, task6.State)

	// test cannot insert task with dup key
	_, err = gm.CreateTask(ctx, "key1", "test2", 4, "", []byte("test2"))
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'key1' for key 'tidb_global_task.task_key'")

	// test cancel task
	id, err = gm.CreateTask(ctx, "key2", "test", 4, "", []byte("test"))
	require.NoError(t, err)

	cancelling, err := testutil.IsTaskCancelling(ctx, gm, id)
	require.NoError(t, err)
	require.False(t, cancelling)

	require.NoError(t, gm.CancelTask(ctx, id))
	cancelling, err = testutil.IsTaskCancelling(ctx, gm, id)
	require.NoError(t, err)
	require.True(t, cancelling)

	id, err = gm.CreateTask(ctx, "key-fail", "test2", 4, "", []byte("test2"))
	require.NoError(t, err)
	// state not right, update nothing
	require.NoError(t, gm.FailTask(ctx, id, proto.TaskStateRunning, errors.New("test error")))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePending, task.State)
	require.Nil(t, task.Error)
	curTime := time.Unix(time.Now().Unix(), 0)
	require.NoError(t, gm.FailTask(ctx, id, proto.TaskStatePending, errors.New("test error")))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateFailed, task.State)
	require.ErrorContains(t, task.Error, "test error")
	endTime, err := testutil.GetTaskEndTime(ctx, gm, id)
	require.NoError(t, err)
	require.LessOrEqual(t, endTime.Sub(curTime), time.Since(curTime))
	require.GreaterOrEqual(t, endTime, curTime)

	// succeed a pending task, no effect
	id, err = gm.CreateTask(ctx, "key-success", "test", 4, "", []byte("test"))
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

	// reverted a pending task, no effect
	id, err = gm.CreateTask(ctx, "key-reverted", "test", 4, "", []byte("test"))
	require.NoError(t, err)
	require.NoError(t, gm.RevertedTask(ctx, id))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStatePending, proto.StepInit)
	// reverted a reverting task
	require.NoError(t, gm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, nil))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateRunning, proto.StepOne)
	task.State = proto.TaskStateReverting
	err = gm.RevertTask(ctx, task.ID, proto.TaskStateRunning, errors.New("test error"))
	require.NoError(t, err)
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateReverting, task.State)
	require.ErrorContains(t, task.Error, "test error")
	require.NoError(t, gm.RevertedTask(ctx, task.ID))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateReverted, task.State)

	// paused
	id, err = gm.CreateTask(ctx, "key-paused", "test", 4, "", []byte("test"))
	require.NoError(t, err)
	require.NoError(t, gm.PausedTask(ctx, id))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStatePending, proto.StepInit)
	// reverted a reverting task
	task.State = proto.TaskStatePausing
	found, err := gm.PauseTask(ctx, task.Key)
	require.NoError(t, err)
	require.True(t, found)
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePausing, task.State)
	require.NoError(t, gm.PausedTask(ctx, task.ID))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePaused, task.State)
}

func checkAfterSwitchStep(t *testing.T, startTime time.Time, task *proto.Task, subtasks []*proto.Subtask, step proto.Step) {
	tm, err := storage.GetTaskManager()
	require.NoError(t, err)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	checkTaskStateStep(t, task, proto.TaskStateRunning, step)
	require.GreaterOrEqual(t, task.StartTime, startTime)
	require.GreaterOrEqual(t, task.StateUpdateTime, startTime)
	gotSubtasks, err := tm.GetAllSubtasksByStepAndState(ctx, task.ID, task.Step, proto.SubtaskStatePending)
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
	store, tm, ctx := testutil.InitTableTest(t)
	tk := testkit.NewTestKit(t, store)

	require.NoError(t, tm.InitMeta(ctx, ":4000", ""))
	taskID, err := tm.CreateTask(ctx, "key1", "test", 4, "", []byte("test"))
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
	// some fields are changed in prev call, change back.
	task.State = proto.TaskStatePending
	task.Step = proto.StepInit
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
	store, tm, ctx := testutil.InitTableTest(t)
	tk := testkit.NewTestKit(t, store)

	require.NoError(t, tm.InitMeta(ctx, ":4000", ""))
	// normal flow
	prepare := func(taskKey string) (*proto.Task, []*proto.Subtask) {
		taskID, err := tm.CreateTask(ctx, taskKey, "test", 4, "", []byte("test"))
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

	// mock another scheduler inserted some subtasks
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/storage/waitBeforeInsertSubtasks", `1*return(true)`)
	task2, subtasks2 := prepare("key2")
	go func() {
		storage.TestChannel <- struct{}{}
		tk2 := testkit.NewTestKit(t, store)
		subtask := subtasks2[0]
		_, err = sqlexec.ExecSQL(ctx, tk2.Session(), `
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
	gotSubtasks, err := tm.GetAllSubtasksByStepAndState(ctx, task2.ID, proto.StepOne, proto.SubtaskStatePending)
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
		_, err = sqlexec.ExecSQL(ctx, tk.Session(), `
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
	_, gm, ctx := testutil.InitTableTest(t)

	bak := proto.MaxConcurrentTask
	t.Cleanup(func() {
		proto.MaxConcurrentTask = bak
	})
	proto.MaxConcurrentTask = 4
	require.NoError(t, gm.InitMeta(ctx, ":4000", ""))
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
		_, err := gm.CreateTask(ctx, taskKey, "test", 4, "", []byte("test"))
		require.NoError(t, err)
		require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
			_, err := se.GetSQLExecutor().ExecuteInternal(ctx, `
				update mysql.tidb_global_task set state = %? where task_key = %?`,
				state, taskKey)
			return err
		}))
	}
	// adjust task order
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		_, err := se.GetSQLExecutor().ExecuteInternal(ctx, `
				update mysql.tidb_global_task set create_time = current_timestamp`)
		return err
	}))
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		_, err := se.GetSQLExecutor().ExecuteInternal(ctx, `
				update mysql.tidb_global_task
				set create_time = timestampadd(minute, -10, current_timestamp)
				where task_key = 'key/5'`)
		return err
	}))
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		_, err := se.GetSQLExecutor().ExecuteInternal(ctx, `
				update mysql.tidb_global_task set priority = 100 where task_key = 'key/6'`)
		return err
	}))
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
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
	}
	require.Equal(t, []string{"key/6", "key/5", "key/1", "key/2", "key/3", "key/4", "key/8", "key/9"}, taskKeys)
}

func TestGetUsedSlotsOnNodes(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)

	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb-1", []byte(""), proto.SubtaskStateRunning, "test", 12)
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb-2", []byte(""), proto.SubtaskStatePending, "test", 12)
	testutil.InsertSubtask(t, sm, 2, proto.StepOne, "tidb-2", []byte(""), proto.SubtaskStatePending, "test", 8)
	testutil.InsertSubtask(t, sm, 3, proto.StepOne, "tidb-3", []byte(""), proto.SubtaskStatePending, "test", 8)
	testutil.InsertSubtask(t, sm, 4, proto.StepOne, "tidb-3", []byte(""), proto.SubtaskStateFailed, "test", 8)
	slotsOnNodes, err := sm.GetUsedSlotsOnNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]int{
		"tidb-1": 12,
		"tidb-2": 20,
		"tidb-3": 8,
	}, slotsOnNodes)
}

func TestGetActiveSubtasks(t *testing.T) {
	_, tm, ctx := testutil.InitTableTest(t)
	require.NoError(t, tm.InitMeta(ctx, ":4000", ""))
	id, err := tm.CreateTask(ctx, "key1", "test", 4, "", []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	task, err := tm.GetTaskByID(ctx, id)
	require.NoError(t, err)

	subtasks := make([]*proto.Subtask, 0, 3)
	for i := 0; i < 3; i++ {
		subtasks = append(subtasks,
			proto.NewSubtask(proto.StepOne, id, "test", fmt.Sprintf("tidb%d", i), 8, []byte("{}}"), i+1),
		)
	}
	require.NoError(t, tm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, subtasks))
	require.NoError(t, tm.FinishSubtask(ctx, "tidb0", 1, []byte("{}}")))
	require.NoError(t, tm.StartSubtask(ctx, 2, "tidb1"))

	activeSubtasks, err := tm.GetActiveSubtasks(ctx, task.ID)
	require.NoError(t, err)
	require.Len(t, activeSubtasks, 2)
	slices.SortFunc(activeSubtasks, func(i, j *proto.SubtaskBase) int {
		return int(i.ID - j.ID)
	})
	require.Equal(t, int64(2), activeSubtasks[0].ID)
	require.Equal(t, proto.SubtaskStateRunning, activeSubtasks[0].State)
	require.Equal(t, int64(3), activeSubtasks[1].ID)
	require.Equal(t, proto.SubtaskStatePending, activeSubtasks[1].State)
}

func TestSubTaskTable(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)
	timeBeforeCreate := time.Unix(time.Now().Unix(), 0)
	require.NoError(t, sm.InitMeta(ctx, ":4000", ""))
	id, err := sm.CreateTask(ctx, "key1", "test", 4, "", []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	err = sm.SwitchTaskStep(
		ctx,
		&proto.Task{TaskBase: proto.TaskBase{ID: 1, State: proto.TaskStatePending, Step: proto.StepInit}},
		proto.TaskStateRunning,
		proto.StepOne,
		[]*proto.Subtask{proto.NewSubtask(proto.StepOne, 1, proto.TaskTypeExample, "tidb1", 11, []byte("test"), 1)},
	)
	require.NoError(t, err)

	nilTask, err := sm.GetFirstSubtaskInStates(ctx, "tidb2", 1, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Nil(t, nilTask)

	subtask, err := sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, proto.StepOne, subtask.Step)
	require.Equal(t, proto.TaskTypeExample, subtask.Type)
	require.Equal(t, int64(1), subtask.TaskID)
	require.Equal(t, proto.SubtaskStatePending, subtask.State)
	require.Equal(t, "tidb1", subtask.ExecID)
	require.Equal(t, []byte("test"), subtask.Meta)
	require.Equal(t, 11, subtask.Concurrency)
	require.GreaterOrEqual(t, subtask.CreateTime, timeBeforeCreate)
	require.Equal(t, 1, subtask.Ordinal)
	require.Zero(t, subtask.StartTime)
	require.Zero(t, subtask.UpdateTime)
	require.Equal(t, "{}", subtask.Summary)

	subtask2, err := sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, subtask, subtask2)

	cntByStates, err := sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepOne)
	require.NoError(t, err)
	require.Len(t, cntByStates, 1)
	require.Equal(t, int64(1), cntByStates[proto.SubtaskStatePending])

	ok, err := sm.HasSubtasksInStates(ctx, "tidb1", 1, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.True(t, ok)

	ts := time.Now()
	time.Sleep(time.Second)
	require.NoError(t, sm.StartSubtask(ctx, 1, "tidb1"))

	err = sm.StartSubtask(ctx, 1, "tidb2")
	require.Error(t, storage.ErrSubtaskNotFound, err)

	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Nil(t, subtask)

	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepOne, proto.SubtaskStateRunning)
	require.NoError(t, err)
	require.Equal(t, proto.TaskTypeExample, subtask.Type)
	require.Equal(t, int64(1), subtask.TaskID)
	require.Equal(t, proto.SubtaskStateRunning, subtask.State)
	require.Equal(t, "tidb1", subtask.ExecID)
	require.Equal(t, []byte("test"), subtask.Meta)
	require.GreaterOrEqual(t, subtask.StartTime, ts)
	require.GreaterOrEqual(t, subtask.UpdateTime, ts)

	// check update time after state change to cancel
	time.Sleep(time.Second)
	require.NoError(t, sm.FailSubtask(ctx, "tidb1", 1, errors.New("mock err")))
	subtask2, err = sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepOne, proto.SubtaskStateFailed)
	require.NoError(t, err)
	require.Equal(t, proto.SubtaskStateFailed, subtask2.State)
	require.Greater(t, subtask2.UpdateTime, subtask.UpdateTime)

	cntByStates, err = sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepOne)
	require.NoError(t, err)
	require.Equal(t, int64(0), cntByStates[proto.SubtaskStatePending])

	ok, err = sm.HasSubtasksInStates(ctx, "tidb1", 1, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, testutil.DeleteSubtasksByTaskID(ctx, sm, 1))

	ok, err = sm.HasSubtasksInStates(ctx, "tidb1", 1, proto.StepOne, proto.SubtaskStatePending, proto.SubtaskStateRunning)
	require.NoError(t, err)
	require.False(t, ok)

	testutil.CreateSubTask(t, sm, 2, proto.StepOne, "tidb1", []byte("test"), proto.TaskTypeExample, 11)

	subtasks, err := sm.GetAllSubtasksByStepAndState(ctx, 2, proto.StepOne, proto.SubtaskStateSucceed)
	require.NoError(t, err)
	require.Len(t, subtasks, 0)

	subtasks, err = testutil.GetSubtasksByTaskID(ctx, sm, 2)
	require.NoError(t, err)
	require.Len(t, subtasks, 1)
	subtaskID := subtasks[0].ID
	require.NoError(t, sm.FinishSubtask(ctx, "tidb1", subtaskID, []byte{}))

	subtasks, err = sm.GetAllSubtasksByStepAndState(ctx, 2, proto.StepOne, proto.SubtaskStateSucceed)
	require.NoError(t, err)
	require.Len(t, subtasks, 1)

	rowCount, err := sm.GetSubtaskRowCount(ctx, 2, proto.StepOne)
	require.NoError(t, err)
	require.Equal(t, int64(0), rowCount)
	require.NoError(t, sm.UpdateSubtaskRowCount(ctx, subtaskID, 100))
	rowCount, err = sm.GetSubtaskRowCount(ctx, 2, proto.StepOne)
	require.NoError(t, err)
	require.Equal(t, int64(100), rowCount)

	getSubtaskBaseSlice := func(sts []*proto.Subtask) []*proto.SubtaskBase {
		res := make([]*proto.SubtaskBase, 0, len(sts))
		for _, st := range sts {
			res = append(res, &st.SubtaskBase)
		}
		return res
	}
	// test UpdateSubtasksExecIDs
	// 1. update one subtask
	testutil.CreateSubTask(t, sm, 5, proto.StepOne, "tidb1", []byte("test"), proto.TaskTypeExample, 11)
	subtasks, err = sm.GetAllSubtasksByStepAndState(ctx, 5, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	subtasks[0].ExecID = "tidb2"
	subtaskBases := getSubtaskBaseSlice(subtasks)
	require.NoError(t, sm.UpdateSubtasksExecIDs(ctx, subtaskBases))
	subtasks, err = sm.GetAllSubtasksByStepAndState(ctx, 5, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb2", subtasks[0].ExecID)
	// 2. update 2 subtasks
	testutil.CreateSubTask(t, sm, 5, proto.StepOne, "tidb1", []byte("test"), proto.TaskTypeExample, 11)
	subtasks, err = sm.GetAllSubtasksByStepAndState(ctx, 5, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	subtasks[0].ExecID = "tidb3"
	subtaskBases = getSubtaskBaseSlice(subtasks)
	require.NoError(t, sm.UpdateSubtasksExecIDs(ctx, subtaskBases))
	subtasks, err = sm.GetAllSubtasksByStepAndState(ctx, 5, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb3", subtasks[0].ExecID)
	require.Equal(t, "tidb1", subtasks[1].ExecID)
	// update fail
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", subtasks[0].ID, proto.SubtaskStateRunning, nil))
	subtasks, err = sm.GetAllSubtasksByStepAndState(ctx, 5, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb3", subtasks[0].ExecID)
	subtasks[0].ExecID = "tidb2"
	// update success
	subtaskBases = getSubtaskBaseSlice(subtasks)
	require.NoError(t, sm.UpdateSubtasksExecIDs(ctx, subtaskBases))
	subtasks, err = sm.GetAllSubtasksByStepAndState(ctx, 5, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb2", subtasks[0].ExecID)

	// test GetSubtaskErrors
	testutil.CreateSubTask(t, sm, 7, proto.StepOne, "tidb1", []byte("test"), proto.TaskTypeExample, 11)
	subtasks, err = sm.GetAllSubtasksByStepAndState(ctx, 7, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 1, len(subtasks))
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", subtasks[0].ID, proto.SubtaskStateFailed, errors.New("test err")))
	subtaskErrs, err := sm.GetSubtaskErrors(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, 1, len(subtaskErrs))
	require.ErrorContains(t, subtaskErrs[0], "test err")
}

func TestBothTaskAndSubTaskTable(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)
	require.NoError(t, sm.InitMeta(ctx, ":4000", ""))
	id, err := sm.CreateTask(ctx, "key1", "test", 4, "", []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := testutil.GetOneTask(ctx, sm)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePending, task.State)

	// isSubTaskRevert: false
	subTasks := []*proto.Subtask{
		proto.NewSubtask(proto.StepOne, task.ID, proto.TaskTypeExample, "instance1", 1, []byte("m1"), 1),
		proto.NewSubtask(proto.StepOne, task.ID, proto.TaskTypeExample, "instance2", 1, []byte("m2"), 2),
	}
	err = sm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, subTasks)
	require.NoError(t, err)

	task, err = sm.GetTaskByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateRunning, task.State)

	subtask1, err := sm.GetFirstSubtaskInStates(ctx, "instance1", 1, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(1), subtask1.ID)
	require.Equal(t, proto.TaskTypeExample, subtask1.Type)
	require.Equal(t, []byte("m1"), subtask1.Meta)

	subtask2, err := sm.GetFirstSubtaskInStates(ctx, "instance2", 1, proto.StepOne, proto.SubtaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(2), subtask2.ID)
	require.Equal(t, proto.TaskTypeExample, subtask2.Type)
	require.Equal(t, []byte("m2"), subtask2.Meta)

	cntByStates, err := sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepOne)
	require.NoError(t, err)
	require.Len(t, cntByStates, 1)
	require.Equal(t, int64(2), cntByStates[proto.SubtaskStatePending])
}

func TestGetSubtaskCntByStates(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb1", nil, proto.SubtaskStatePending, "test", 1)
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb1", nil, proto.SubtaskStatePending, "test", 1)
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb1", nil, proto.SubtaskStateRunning, "test", 1)
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb1", nil, proto.SubtaskStateSucceed, "test", 1)
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "tidb1", nil, proto.SubtaskStateFailed, "test", 1)
	testutil.InsertSubtask(t, sm, 1, proto.StepTwo, "tidb1", nil, proto.SubtaskStateFailed, "test", 1)
	cntByStates, err := sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepOne)
	require.NoError(t, err)
	require.Len(t, cntByStates, 4)
	require.Equal(t, int64(2), cntByStates[proto.SubtaskStatePending])
	require.Equal(t, int64(1), cntByStates[proto.SubtaskStateRunning])
	require.Equal(t, int64(1), cntByStates[proto.SubtaskStateSucceed])
	require.Equal(t, int64(1), cntByStates[proto.SubtaskStateFailed])
	cntByStates, err = sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepTwo)
	require.NoError(t, err)
	require.Len(t, cntByStates, 1)
	require.Equal(t, int64(1), cntByStates[proto.SubtaskStateFailed])
}

func TestDistFrameworkMeta(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)

	// when no node
	_, err := sm.GetCPUCountOfNode(ctx)
	require.ErrorContains(t, err, "no managed nodes")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(0)")
	require.NoError(t, sm.InitMeta(ctx, ":4000", "background"))
	_, err = sm.GetCPUCountOfNode(ctx)
	require.ErrorContains(t, err, "no managed node have enough resource")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(100)")
	require.NoError(t, sm.InitMeta(ctx, ":4000", "background"))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)")
	require.NoError(t, sm.InitMeta(ctx, ":4001", ""))
	require.NoError(t, sm.InitMeta(ctx, ":4002", "background"))
	nodes, err := sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "background", CPUCount: 100},
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "background", CPUCount: 8},
	}, nodes)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(100)")
	require.NoError(t, sm.InitMeta(ctx, ":4002", ""))
	require.NoError(t, sm.InitMeta(ctx, ":4003", "background"))

	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4000", Role: "background", CPUCount: 100},
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "", CPUCount: 100},
		{ID: ":4003", Role: "background", CPUCount: 100},
	}, nodes)
	cpuCount, err := sm.GetCPUCountOfNode(ctx)
	require.NoError(t, err)
	require.Equal(t, 100, cpuCount)

	require.NoError(t, sm.InitMeta(ctx, ":4002", "background"))

	require.NoError(t, sm.DeleteDeadNodes(ctx, []string{":4000"}))
	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "background", CPUCount: 100},
		{ID: ":4003", Role: "background", CPUCount: 100},
	}, nodes)

	require.NoError(t, sm.DeleteDeadNodes(ctx, []string{":4003"}))
	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "background", CPUCount: 100},
	}, nodes)

	require.NoError(t, sm.DeleteDeadNodes(ctx, []string{":4002"}))
	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4001", Role: "", CPUCount: 8},
	}, nodes)
	cpuCount, err = sm.GetCPUCountOfNode(ctx)
	require.NoError(t, err)
	require.Equal(t, 8, cpuCount)

	require.NoError(t, sm.RecoverMeta(ctx, ":4002", "background"))
	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "background", CPUCount: 100},
	}, nodes)
	// should not reset role
	require.NoError(t, sm.RecoverMeta(ctx, ":4002", ""))
	nodes, err = sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []proto.ManagedNode{
		{ID: ":4001", Role: "", CPUCount: 8},
		{ID: ":4002", Role: "background", CPUCount: 100},
	}, nodes)
}

func TestSubtaskHistoryTable(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)

	const (
		taskID       = 1
		taskID2      = 2
		tidb1        = "tidb1"
		tidb2        = "tidb2"
		tidb3        = "tidb3"
		meta         = "test"
		finishedMeta = "finished"
	)

	subTask1 := testutil.CreateSubTask(t, sm, taskID, proto.StepInit, tidb1, []byte(meta), proto.TaskTypeExample, 11)
	require.NoError(t, sm.FinishSubtask(ctx, tidb1, subTask1, []byte(finishedMeta)))
	subTask2 := testutil.CreateSubTask(t, sm, taskID, proto.StepInit, tidb2, []byte(meta), proto.TaskTypeExample, 11)
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, tidb2, subTask2, proto.SubtaskStateCanceled, nil))
	subTask3 := testutil.CreateSubTask(t, sm, taskID, proto.StepInit, tidb3, []byte(meta), proto.TaskTypeExample, 11)
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, tidb3, subTask3, proto.SubtaskStateFailed, nil))

	subTasks, err := testutil.GetSubtasksByTaskID(ctx, sm, taskID)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)
	historySubTasksCnt, err := testutil.GetSubtasksFromHistory(ctx, sm)
	require.NoError(t, err)
	require.Equal(t, 0, historySubTasksCnt)
	subTasks, err = sm.GetSubtasksWithHistory(ctx, taskID, proto.StepInit)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)

	// test TransferSubTasks2History
	require.NoError(t, testutil.TransferSubTasks2History(ctx, sm, taskID))

	subTasks, err = testutil.GetSubtasksByTaskID(ctx, sm, taskID)
	require.NoError(t, err)
	require.Len(t, subTasks, 0)
	historySubTasksCnt, err = testutil.GetSubtasksFromHistory(ctx, sm)
	require.NoError(t, err)
	require.Equal(t, 3, historySubTasksCnt)
	subTasks, err = sm.GetSubtasksWithHistory(ctx, taskID, proto.StepInit)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)

	// test GC history table.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds", "return(1)")
	time.Sleep(2 * time.Second)

	subTask4 := testutil.CreateSubTask(t, sm, taskID2, proto.StepInit, tidb1, []byte(meta), proto.TaskTypeExample, 11)
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, tidb1, subTask4, proto.SubtaskStateFailed, nil))
	require.NoError(t, testutil.TransferSubTasks2History(ctx, sm, taskID2))

	require.NoError(t, sm.GCSubtasks(ctx))

	historySubTasksCnt, err = testutil.GetSubtasksFromHistory(ctx, sm)
	require.NoError(t, err)
	require.Equal(t, 1, historySubTasksCnt)
}

func TestTaskHistoryTable(t *testing.T) {
	_, gm, ctx := testutil.InitTableTest(t)

	require.NoError(t, gm.InitMeta(ctx, ":4000", ""))
	_, err := gm.CreateTask(ctx, "1", proto.TaskTypeExample, 1, "", nil)
	require.NoError(t, err)
	taskID, err := gm.CreateTask(ctx, "2", proto.TaskTypeExample, 1, "", nil)
	require.NoError(t, err)

	tasks, err := gm.GetTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 2, len(tasks))
	testutil.InsertSubtask(t, gm, tasks[0].ID, proto.StepOne, "tidb1", proto.EmptyMeta, proto.SubtaskStateRunning, proto.TaskTypeExample, 1)
	testutil.InsertSubtask(t, gm, tasks[1].ID, proto.StepOne, "tidb1", proto.EmptyMeta, proto.SubtaskStateRunning, proto.TaskTypeExample, 1)
	oldTasks := tasks
	require.NoError(t, gm.TransferTasks2History(ctx, tasks))

	tasks, err = gm.GetTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 0, len(tasks))
	num, err := testutil.GetTasksFromHistory(ctx, gm)
	require.NoError(t, err)
	require.Equal(t, 2, num)
	num, err = testutil.GetSubtasksFromHistoryByTaskID(ctx, gm, oldTasks[0].ID)
	require.NoError(t, err)
	require.Equal(t, 1, num)
	num, err = testutil.GetSubtasksFromHistoryByTaskID(ctx, gm, oldTasks[1].ID)
	require.NoError(t, err)
	require.Equal(t, 1, num)

	task, err := gm.GetTaskByIDWithHistory(ctx, taskID)
	require.NoError(t, err)
	require.NotNil(t, task)

	task, err = gm.GetTaskByKeyWithHistory(ctx, "1")
	require.NoError(t, err)
	require.NotNil(t, task)

	// task with fail transfer
	_, err = gm.CreateTask(ctx, "3", proto.TaskTypeExample, 1, "", nil)
	require.NoError(t, err)
	tasks, err = gm.GetTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	tasks[0].Error = errors.New("mock err")
	require.NoError(t, gm.TransferTasks2History(ctx, tasks))
	num, err = testutil.GetTasksFromHistory(ctx, gm)
	require.NoError(t, err)
	require.Equal(t, 3, num)
}

func TestPauseAndResume(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)

	testutil.CreateSubTask(t, sm, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11)
	testutil.CreateSubTask(t, sm, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11)
	testutil.CreateSubTask(t, sm, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11)
	// 1.1 pause all subtasks.
	require.NoError(t, sm.PauseSubtasks(ctx, "tidb1", 1))
	cntByStates, err := sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepInit)
	require.NoError(t, err)
	require.Equal(t, int64(3), cntByStates[proto.SubtaskStatePaused])
	// 1.2 resume all subtasks.
	require.NoError(t, sm.ResumeSubtasks(ctx, 1))
	cntByStates, err = sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepInit)
	require.NoError(t, err)
	require.Equal(t, int64(3), cntByStates[proto.SubtaskStatePending])

	// 2.1 pause 2 subtasks.
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", 1, proto.SubtaskStateSucceed, nil))
	require.NoError(t, sm.PauseSubtasks(ctx, "tidb1", 1))
	cntByStates, err = sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepInit)
	require.NoError(t, err)
	require.Equal(t, int64(2), cntByStates[proto.SubtaskStatePaused])
	// 2.2 resume 2 subtasks.
	require.NoError(t, sm.ResumeSubtasks(ctx, 1))
	cntByStates, err = sm.GetSubtaskCntGroupByStates(ctx, 1, proto.StepInit)
	require.NoError(t, err)
	require.Equal(t, int64(2), cntByStates[proto.SubtaskStatePending])
}

func TestCancelAndExecIdChanged(t *testing.T) {
	sm, ctx, cancel := testutil.InitTableTestWithCancel(t)

	testutil.CreateSubTask(t, sm, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, 11)
	subtask, err := sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.SubtaskStatePending)
	require.NoError(t, err)
	// 1. cancel the ctx, then update subtask state.
	cancel()
	require.ErrorIs(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", subtask.ID, proto.SubtaskStateFailed, nil), context.Canceled)
	ctx = context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.SubtaskStatePending)
	// task state not changed
	require.NoError(t, err)
	require.Equal(t, proto.SubtaskStatePending, subtask.State)

	// 2. change the exec_id
	// exec_id changed
	require.NoError(t, testutil.UpdateSubtaskExecID(ctx, sm, "tidb2", subtask.ID))
	// exec_id in memory unchanged, call UpdateSubtaskStateAndError.
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, subtask.ExecID, subtask.ID, proto.SubtaskStateFailed, nil))
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb2", 1, proto.StepInit, proto.SubtaskStatePending)
	require.NoError(t, err)
	// state unchanged
	require.NotNil(t, subtask)
}

func TestTaskNotFound(t *testing.T) {
	_, gm, ctx := testutil.InitTableTest(t)
	task, err := gm.GetTaskByID(ctx, 1)
	require.Error(t, err, storage.ErrTaskNotFound)
	require.Nil(t, task)
	task, err = gm.GetTaskByIDWithHistory(ctx, 1)
	require.Error(t, err, storage.ErrTaskNotFound)
	require.Nil(t, task)
	task, err = gm.GetTaskByKey(ctx, "key")
	require.Error(t, err, storage.ErrTaskNotFound)
	require.Nil(t, task)
	task, err = gm.GetTaskByKeyWithHistory(ctx, "key")
	require.Error(t, err, storage.ErrTaskNotFound)
	require.Nil(t, task)
}

func TestInitMeta(t *testing.T) {
	store, sm, ctx := testutil.InitTableTest(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, sm.InitMeta(ctx, "tidb1", ""))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host="tidb1"`).Check(testkit.Rows(""))
	require.NoError(t, sm.InitMeta(ctx, "tidb1", "background"))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host="tidb1"`).Check(testkit.Rows("background"))
	tk.MustExec(`set global tidb_service_scope=""`)
	tk.MustQuery("select @@global.tidb_service_scope").Check(testkit.Rows(""))

	// 1. delete then start.
	require.NoError(t, sm.DeleteDeadNodes(ctx, []string{"tidb1"}))
	require.NoError(t, sm.InitMeta(ctx, "tidb1", ""))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host="tidb1"`).Check(testkit.Rows(""))

	require.NoError(t, sm.DeleteDeadNodes(ctx, []string{"tidb1"}))
	require.NoError(t, sm.InitMeta(ctx, "tidb1", "background"))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host="tidb1"`).Check(testkit.Rows("background"))

	// 2. delete then set.
	require.NoError(t, sm.DeleteDeadNodes(ctx, []string{"tidb1"}))
	tk.MustExec(`set global tidb_service_scope=""`)
	tk.MustQuery("select @@global.tidb_service_scope").Check(testkit.Rows(""))

	require.NoError(t, sm.DeleteDeadNodes(ctx, []string{"tidb1"}))
	tk.MustExec(`set global tidb_service_scope="background"`)
	tk.MustQuery("select @@global.tidb_service_scope").Check(testkit.Rows("background"))
}

func TestSubtaskType(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)
	cases := []proto.TaskType{
		proto.TaskTypeExample,
		proto.ImportInto,
		proto.Backfill,
		"",
	}
	for i, c := range cases {
		testutil.InsertSubtask(t, sm, int64(i+1), proto.StepOne, "tidb-1", []byte(""), proto.SubtaskStateRunning, c, 12)
		subtask, err := sm.GetFirstSubtaskInStates(ctx, "tidb-1", int64(i+1), proto.StepOne, proto.SubtaskStateRunning)
		require.NoError(t, err)
		require.Equal(t, c, subtask.Type)
	}
}

func TestRunningSubtasksBack2Pending(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)
	subtasks := []*proto.Subtask{
		{SubtaskBase: proto.SubtaskBase{TaskID: 1, ExecID: "tidb-1", State: proto.SubtaskStatePending}},
		{SubtaskBase: proto.SubtaskBase{TaskID: 1, ExecID: "tidb-1", State: proto.SubtaskStateRunning}},
		{SubtaskBase: proto.SubtaskBase{TaskID: 1, ExecID: "tidb-2", State: proto.SubtaskStatePending}},
		{SubtaskBase: proto.SubtaskBase{TaskID: 2, ExecID: "tidb-1", State: proto.SubtaskStatePending}},
	}
	for _, st := range subtasks {
		testutil.InsertSubtask(t, sm, st.TaskID, proto.StepOne, st.ExecID, []byte(""), st.State, proto.TaskTypeExample, 12)
	}

	getAllSubtasks := func() []*proto.Subtask {
		res := make([]*proto.Subtask, 0, 3)
		require.NoError(t, sm.WithNewSession(func(se sessionctx.Context) error {
			rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
				select cast(task_key as signed), exec_id, state, state_update_time
				from mysql.tidb_background_subtask
				order by task_key, exec_id, state`)
			require.NoError(t, err)
			for _, r := range rs {
				var updateTime time.Time
				if !r.IsNull(3) {
					updateTime = time.Unix(r.GetInt64(3), 0)
				}
				res = append(res, &proto.Subtask{
					SubtaskBase: proto.SubtaskBase{
						TaskID: r.GetInt64(0),
						ExecID: r.GetString(1),
						State:  proto.SubtaskState(r.GetString(2)),
					},
					UpdateTime: updateTime,
				})
			}
			return nil
		}))
		return res
	}

	require.Equal(t, subtasks, getAllSubtasks())
	require.NoError(t, sm.RunningSubtasksBack2Pending(ctx, nil))
	require.Equal(t, subtasks, getAllSubtasks())

	activeSubtasks, err := sm.GetActiveSubtasks(ctx, 1)
	require.NoError(t, err)
	require.Len(t, activeSubtasks, 3)
	startTime := time.Unix(time.Now().Unix(), 0)
	// this list contains running and pending subtasks, just for test.
	require.NoError(t, sm.RunningSubtasksBack2Pending(ctx, activeSubtasks))
	allSubtasks := getAllSubtasks()
	require.GreaterOrEqual(t, allSubtasks[1].UpdateTime, startTime)
	allSubtasks[1].UpdateTime = time.Time{}
	subtasks[1].State = proto.SubtaskStatePending
	require.Equal(t, subtasks, allSubtasks)
}

func TestSubtasksState(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)
	ts := time.Now()
	time.Sleep(1 * time.Second)
	// 1. test FailSubtask do update start/update time
	testutil.CreateSubTask(t, sm, 3, proto.StepInit, "for_test", []byte("test"), proto.TaskTypeExample, 11)
	require.NoError(t, sm.FailSubtask(ctx, "for_test", 3, errors.New("fail")))
	subtask, err := sm.GetFirstSubtaskInStates(ctx, "for_test", 3, proto.StepInit, proto.SubtaskStateFailed)
	require.NoError(t, err)
	require.Equal(t, proto.SubtaskStateFailed, subtask.State)
	require.Greater(t, subtask.StartTime, ts)
	require.Greater(t, subtask.UpdateTime, ts)

	endTime, err := testutil.GetSubtaskEndTime(ctx, sm, subtask.ID)
	require.NoError(t, err)
	require.Greater(t, endTime, ts)

	// 2. test FinishSubtask do update update time
	testutil.CreateSubTask(t, sm, 4, proto.StepInit, "for_test1", []byte("test"), proto.TaskTypeExample, 11)
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "for_test1", 4, proto.StepInit, proto.SubtaskStatePending)
	require.NoError(t, err)
	err = sm.StartSubtask(ctx, subtask.ID, "for_test1")
	require.NoError(t, err)

	subtask, err = sm.GetFirstSubtaskInStates(ctx, "for_test1", 4, proto.StepInit, proto.SubtaskStateRunning)
	require.NoError(t, err)
	require.Greater(t, subtask.StartTime, ts)
	require.Greater(t, subtask.UpdateTime, ts)
	ts = time.Now()
	time.Sleep(time.Second)
	require.NoError(t, sm.FinishSubtask(ctx, "for_test1", subtask.ID, []byte{}))
	subtask2, err := sm.GetFirstSubtaskInStates(ctx, "for_test1", 4, proto.StepInit, proto.SubtaskStateSucceed)
	require.NoError(t, err)
	require.Equal(t, subtask2.StartTime, subtask.StartTime)
	require.Greater(t, subtask2.UpdateTime, subtask.UpdateTime)
	endTime, err = testutil.GetSubtaskEndTime(ctx, sm, subtask.ID)
	require.NoError(t, err)
	require.Greater(t, endTime, ts)

	// 3. test CancelSubtask
	testutil.CreateSubTask(t, sm, 3, proto.StepInit, "for_test", []byte("test"), proto.TaskTypeExample, 11)
	require.NoError(t, sm.CancelSubtask(ctx, "for_test", 3))
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "for_test", 3, proto.StepInit, proto.SubtaskStateCanceled)
	require.NoError(t, err)
	require.Equal(t, proto.SubtaskStateCanceled, subtask.State)
	require.Greater(t, subtask.StartTime, ts)
	require.Greater(t, subtask.UpdateTime, ts)

	endTime, err = testutil.GetSubtaskEndTime(ctx, sm, subtask.ID)
	require.NoError(t, err)
	require.Greater(t, endTime, ts)
}

func checkBasicTaskEq(t *testing.T, expectedTask, task *proto.TaskBase) {
	require.Equal(t, expectedTask.ID, task.ID)
	require.Equal(t, expectedTask.Key, task.Key)
	require.Equal(t, expectedTask.Type, task.Type)
	require.Equal(t, expectedTask.State, task.State)
	require.Equal(t, expectedTask.Step, task.Step)
	require.Equal(t, expectedTask.Priority, task.Priority)
	require.Equal(t, expectedTask.Concurrency, task.Concurrency)
	require.Equal(t, expectedTask.CreateTime, task.CreateTime)
}

func TestGetActiveTaskExecInfo(t *testing.T) {
	_, tm, ctx := testutil.InitTableTest(t)

	require.NoError(t, tm.InitMeta(ctx, ":4000", ""))
	taskStates := []proto.TaskState{proto.TaskStateRunning, proto.TaskStateReverting, proto.TaskStateReverting, proto.TaskStatePausing}
	tasks := make([]*proto.Task, 0, len(taskStates))
	for i, expectedState := range taskStates {
		taskID, err := tm.CreateTask(ctx, fmt.Sprintf("key-%d", i), proto.TaskTypeExample, 8, "", []byte(""))
		require.NoError(t, err)
		task, err := tm.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		tasks = append(tasks, task)
		require.NoError(t, tm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepTwo, nil))
		task.State = expectedState
		task.Step = proto.StepTwo
		switch expectedState {
		case proto.TaskStateReverting:
			require.NoError(t, tm.RevertTask(ctx, task.ID, proto.TaskStateRunning, nil))
		case proto.TaskStatePausing:
			_, err = tm.PauseTask(ctx, task.Key)
			require.NoError(t, err)
		}
	}
	// mock a pending subtask of step 1, this should not happen, just for test
	testutil.InsertSubtask(t, tm, tasks[0].ID, proto.StepOne, ":4000", []byte("test"), proto.SubtaskStatePending, proto.TaskTypeExample, 4)
	testutil.InsertSubtask(t, tm, tasks[0].ID, proto.StepTwo, ":4000", []byte("test"), proto.SubtaskStatePending, proto.TaskTypeExample, 4)
	testutil.InsertSubtask(t, tm, tasks[0].ID, proto.StepTwo, ":4000", []byte("test"), proto.SubtaskStateRunning, proto.TaskTypeExample, 4)
	testutil.InsertSubtask(t, tm, tasks[0].ID, proto.StepTwo, ":4001", []byte("test"), proto.SubtaskStateSucceed, proto.TaskTypeExample, 4)
	testutil.InsertSubtask(t, tm, tasks[0].ID, proto.StepTwo, ":4001", []byte("test"), proto.SubtaskStatePending, proto.TaskTypeExample, 4)
	// task 1 has no subtask
	testutil.InsertSubtask(t, tm, tasks[2].ID, proto.StepTwo, ":4001", []byte("test"), proto.SubtaskStatePending, proto.TaskTypeExample, 6)
	testutil.InsertSubtask(t, tm, tasks[3].ID, proto.StepTwo, ":4001", []byte("test"), proto.SubtaskStateRunning, proto.TaskTypeExample, 8)

	subtasks, err2 := tm.GetActiveSubtasks(ctx, 1)
	require.NoError(t, err2)
	_ = subtasks
	// :4000
	taskExecInfos, err := tm.GetTaskExecInfoByExecID(ctx, ":4000")
	require.NoError(t, err)
	require.Len(t, taskExecInfos, 1)
	checkBasicTaskEq(t, &tasks[0].TaskBase, taskExecInfos[0].TaskBase)
	require.Equal(t, 4, taskExecInfos[0].SubtaskConcurrency)
	// :4001
	taskExecInfos, err = tm.GetTaskExecInfoByExecID(ctx, ":4001")
	require.NoError(t, err)
	require.Len(t, taskExecInfos, 3)
	checkBasicTaskEq(t, &tasks[0].TaskBase, taskExecInfos[0].TaskBase)
	require.Equal(t, 4, taskExecInfos[0].SubtaskConcurrency)
	checkBasicTaskEq(t, &tasks[2].TaskBase, taskExecInfos[1].TaskBase)
	require.Equal(t, 6, taskExecInfos[1].SubtaskConcurrency)
	checkBasicTaskEq(t, &tasks[3].TaskBase, taskExecInfos[2].TaskBase)
	require.Equal(t, 8, taskExecInfos[2].SubtaskConcurrency)
}
