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
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func GetResourcePool(t *testing.T) *pools.ResourcePool {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	return pool
}

func GetTaskManager(t *testing.T, pool *pools.ResourcePool) *storage.TaskManager {
	manager := storage.NewTaskManager(pool)
	storage.SetTaskManager(manager)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	return manager
}

func TestGlobalTaskTable(t *testing.T) {
	pool := GetResourcePool(t)
	gm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	id, err := gm.AddNewGlobalTask(ctx, "key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := gm.GetNewGlobalTask(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), task.ID)
	require.Equal(t, "key1", task.Key)
	require.Equal(t, proto.TaskType("test"), task.Type)
	require.Equal(t, proto.TaskStatePending, task.State)
	require.Equal(t, uint64(4), task.Concurrency)
	require.Equal(t, []byte("test"), task.Meta)

	task2, err := gm.GetGlobalTaskByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, task, task2)

	task3, err := gm.GetGlobalTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Len(t, task3, 1)
	require.Equal(t, task, task3[0])

	task4, err := gm.GetGlobalTasksInStates(ctx, proto.TaskStatePending, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task4, 1)
	require.Equal(t, task, task4[0])
	require.GreaterOrEqual(t, task4[0].StateUpdateTime, task.StateUpdateTime)

	prevState := task.State
	task.State = proto.TaskStateRunning
	retryable, err := gm.UpdateGlobalTaskAndAddSubTasks(ctx, task, nil, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task5, err := gm.GetGlobalTasksInStates(ctx, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task.State, task5[0].State)

	task6, err := gm.GetGlobalTaskByKey(ctx, "key1")
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task.State, task6.State)

	// test cannot insert task with dup key
	_, err = gm.AddNewGlobalTask(ctx, "key1", "test2", 4, []byte("test2"))
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'key1' for key 'tidb_global_task.task_key'")

	// test cancel global task
	id, err = gm.AddNewGlobalTask(ctx, "key2", "test", 4, []byte("test"))
	require.NoError(t, err)

	cancelling, err := gm.IsGlobalTaskCancelling(ctx, id)
	require.NoError(t, err)
	require.False(t, cancelling)

	require.NoError(t, gm.CancelGlobalTask(ctx, id))
	cancelling, err = gm.IsGlobalTaskCancelling(ctx, id)
	require.NoError(t, err)
	require.True(t, cancelling)
}

func TestSubTaskTable(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	err := sm.AddNewSubTask(ctx, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false)
	require.NoError(t, err)

	nilTask, err := sm.GetFirstSubtaskInStates(ctx, "tidb2", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Nil(t, nilTask)

	subtask, err := sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, proto.TaskTypeExample, subtask.Type)
	require.Equal(t, int64(1), subtask.TaskID)
	require.Equal(t, proto.TaskStatePending, subtask.State)
	require.Equal(t, "tidb1", subtask.SchedulerID)
	require.Equal(t, []byte("test"), subtask.Meta)
	require.Zero(t, subtask.StartTime)
	require.Zero(t, subtask.UpdateTime)

	subtask2, err := sm.GetFirstSubtaskInStates(ctx, "tidb1", 1, proto.StepInit, proto.TaskStatePending, proto.TaskStateReverted)
	require.NoError(t, err)
	require.Equal(t, subtask, subtask2)

	ids, err := sm.GetSchedulerIDsByTaskID(ctx, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, "tidb1", ids[0])

	ids, err = sm.GetSchedulerIDsByTaskID(ctx, 3)
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
	require.Equal(t, "tidb1", subtask.SchedulerID)
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

	err = sm.AddNewSubTask(ctx, 2, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, true)
	require.NoError(t, err)

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
	err = sm.AddNewSubTask(ctx, 3, proto.StepInit, "for_test", []byte("test"), proto.TaskTypeExample, false)
	require.NoError(t, err)
	require.NoError(t, sm.UpdateErrorToSubtask(ctx, "for_test", 3, errors.New("fail")))
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "for_test", 3, proto.StepInit, proto.TaskStateFailed)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateFailed, subtask.State)
	require.Greater(t, subtask.StartTime, ts)
	require.Greater(t, subtask.UpdateTime, ts)

	// test FinishSubtask do update update time
	err = sm.AddNewSubTask(ctx, 4, proto.StepInit, "for_test1", []byte("test"), proto.TaskTypeExample, false)
	require.NoError(t, err)
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

	// test IsSchedulerCanceled
	canceled, err := sm.IsSchedulerCanceled(ctx, "for_test999", 4)
	require.NoError(t, err)
	require.True(t, canceled)
	canceled, err = sm.IsSchedulerCanceled(ctx, "for_test1", 4)
	require.NoError(t, err)
	require.False(t, canceled)
	canceled, err = sm.IsSchedulerCanceled(ctx, "for_test2", 4)
	require.NoError(t, err)
	require.True(t, canceled)

	// test UpdateSubtasksSchedulerIDs
	// 1. update one subtask
	require.NoError(t, sm.AddNewSubTask(ctx, 5, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	subtasks[0].SchedulerID = "tidb2"
	require.NoError(t, sm.UpdateSubtasksSchedulerIDs(ctx, 5, subtasks))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb2", subtasks[0].SchedulerID)
	// 2. update 2 subtasks
	require.NoError(t, sm.AddNewSubTask(ctx, 5, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	subtasks[0].SchedulerID = "tidb3"
	require.NoError(t, sm.UpdateSubtasksSchedulerIDs(ctx, 5, subtasks))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb3", subtasks[0].SchedulerID)
	require.Equal(t, "tidb1", subtasks[1].SchedulerID)
	// update fail
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, "tidb1", subtasks[0].ID, proto.TaskStateRunning, nil))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb3", subtasks[0].SchedulerID)
	subtasks[0].SchedulerID = "tidb2"
	// update success
	require.NoError(t, sm.UpdateSubtasksSchedulerIDs(ctx, 5, subtasks))
	subtasks, err = sm.GetSubtasksByStepAndState(ctx, 5, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, "tidb2", subtasks[0].SchedulerID)
}

func TestBothGlobalAndSubTaskTable(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	id, err := sm.AddNewGlobalTask(ctx, "key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := sm.GetNewGlobalTask(ctx)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePending, task.State)

	// isSubTaskRevert: false
	prevState := task.State
	task.State = proto.TaskStateRunning
	subTasks := []*proto.Subtask{
		{
			Step:        proto.StepInit,
			Type:        proto.TaskTypeExample,
			SchedulerID: "instance1",
			Meta:        []byte("m1"),
		},
		{
			Step:        proto.StepInit,
			Type:        proto.TaskTypeExample,
			SchedulerID: "instance2",
			Meta:        []byte("m2"),
		},
	}
	retryable, err := sm.UpdateGlobalTaskAndAddSubTasks(ctx, task, subTasks, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task, err = sm.GetGlobalTaskByID(ctx, 1)
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
			Step:        proto.StepInit,
			Type:        proto.TaskTypeExample,
			SchedulerID: "instance3",
			Meta:        []byte("m3"),
		},
		{
			Step:        proto.StepInit,
			Type:        proto.TaskTypeExample,
			SchedulerID: "instance4",
			Meta:        []byte("m4"),
		},
	}
	retryable, err = sm.UpdateGlobalTaskAndAddSubTasks(ctx, task, subTasks, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task, err = sm.GetGlobalTaskByID(ctx, 1)
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
	retryable, err = sm.UpdateGlobalTaskAndAddSubTasks(ctx, task, subTasks, prevState)
	require.EqualError(t, err, "updateTaskErr")
	require.Equal(t, true, retryable)

	task, err = sm.GetGlobalTaskByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateReverting, task.State)

	cnt, err = sm.GetSubtaskInStatesCnt(ctx, 1, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)
}

func TestDistFrameworkMeta(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	require.NoError(t, sm.StartManager(ctx, ":4000", "background"))
	require.NoError(t, sm.StartManager(ctx, ":4001", ""))
	require.NoError(t, sm.StartManager(ctx, ":4002", ""))
	require.NoError(t, sm.StartManager(ctx, ":4002", "background"))

	allNodes, err := sm.GetAllNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{":4000", ":4001", ":4002"}, allNodes)

	nodes, err := sm.GetNodesByRole(ctx, "background")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		":4000": true,
		":4002": true,
	}, nodes)

	nodes, err = sm.GetNodesByRole(ctx, "")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		":4001": true,
	}, nodes)

	require.NoError(t, sm.CleanUpMeta(ctx, []string{":4000"}))
	nodes, err = sm.GetNodesByRole(ctx, "background")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		":4002": true,
	}, nodes)

	require.NoError(t, sm.CleanUpMeta(ctx, []string{":4002"}))
	nodes, err = sm.GetNodesByRole(ctx, "background")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{}, nodes)
}

func TestSubtaskHistoryTable(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

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

	require.NoError(t, sm.AddNewSubTask(ctx, taskID, proto.StepInit, tidb1, []byte(meta), proto.TaskTypeExample, false))
	require.NoError(t, sm.FinishSubtask(ctx, tidb1, subTask1, []byte(finishedMeta)))
	require.NoError(t, sm.AddNewSubTask(ctx, taskID, proto.StepInit, tidb2, []byte(meta), proto.TaskTypeExample, false))
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, tidb2, subTask2, proto.TaskStateCanceled, nil))
	require.NoError(t, sm.AddNewSubTask(ctx, taskID, proto.StepInit, tidb3, []byte(meta), proto.TaskTypeExample, false))
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

	require.NoError(t, sm.AddNewSubTask(ctx, taskID2, proto.StepInit, tidb1, []byte(meta), proto.TaskTypeExample, false))
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, tidb1, subTask4, proto.TaskStateFailed, nil))
	require.NoError(t, sm.TransferSubTasks2History(ctx, taskID2))

	require.NoError(t, sm.GCSubtasks(ctx))

	historySubTasksCnt, err = storage.GetSubtasksFromHistoryForTest(ctx, sm)
	require.NoError(t, err)
	require.Equal(t, 1, historySubTasksCnt)
}

func TestTaskHistoryTable(t *testing.T) {
	pool := GetResourcePool(t)
	gm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	_, err := gm.AddNewGlobalTask(ctx, "1", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	taskID, err := gm.AddNewGlobalTask(ctx, "2", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)

	tasks, err := gm.GetGlobalTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 2, len(tasks))

	require.NoError(t, gm.TransferTasks2History(ctx, tasks))

	tasks, err = gm.GetGlobalTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 0, len(tasks))
	num, err := storage.GetTasksFromHistoryForTest(ctx, gm)
	require.NoError(t, err)
	require.Equal(t, 2, num)

	task, err := gm.GetTaskByIDWithHistory(ctx, taskID)
	require.NoError(t, err)
	require.NotNil(t, task)

	task, err = gm.GetGlobalTaskByKeyWithHistory(ctx, "1")
	require.NoError(t, err)
	require.NotNil(t, task)

	// task with fail transfer
	_, err = gm.AddNewGlobalTask(ctx, "3", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	tasks, err = gm.GetGlobalTasksInStates(ctx, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	tasks[0].Error = errors.New("mock err")
	require.NoError(t, gm.TransferTasks2History(ctx, tasks))
	num, err = storage.GetTasksFromHistoryForTest(ctx, gm)
	require.NoError(t, err)
	require.Equal(t, 3, num)
}

func TestPauseAndResume(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")

	require.NoError(t, sm.AddNewSubTask(ctx, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
	require.NoError(t, sm.AddNewSubTask(ctx, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
	require.NoError(t, sm.AddNewSubTask(ctx, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
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
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ctx = util.WithInternalSourceType(ctx, "table_test")

	require.NoError(t, sm.AddNewSubTask(ctx, 1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
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
	require.NoError(t, sm.UpdateSubtaskStateAndError(ctx, subtask.SchedulerID, subtask.ID, proto.TaskStateFailed, nil))
	subtask, err = sm.GetFirstSubtaskInStates(ctx, "tidb2", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	// state unchanged
	require.NotNil(t, subtask)
}
