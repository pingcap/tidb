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
	manager := storage.NewTaskManager(context.Background(), pool)
	storage.SetTaskManager(manager)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	return manager
}

func TestGlobalTaskTable(t *testing.T) {
	pool := GetResourcePool(t)
	gm := GetTaskManager(t, pool)
	defer pool.Close()
	id, err := gm.AddNewGlobalTask("key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := gm.GetNewGlobalTask()
	require.NoError(t, err)
	require.Equal(t, int64(1), task.ID)
	require.Equal(t, "key1", task.Key)
	require.Equal(t, proto.TaskType("test"), task.Type)
	require.Equal(t, proto.TaskStatePending, task.State)
	require.Equal(t, uint64(4), task.Concurrency)
	require.Equal(t, []byte("test"), task.Meta)

	task2, err := gm.GetGlobalTaskByID(1)
	require.NoError(t, err)
	require.Equal(t, task, task2)

	task3, err := gm.GetGlobalTasksInStates(proto.TaskStatePending)
	require.NoError(t, err)
	require.Len(t, task3, 1)
	require.Equal(t, task, task3[0])

	task4, err := gm.GetGlobalTasksInStates(proto.TaskStatePending, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task4, 1)
	require.Equal(t, task, task4[0])
	require.GreaterOrEqual(t, task4[0].StateUpdateTime, task.StateUpdateTime)

	prevState := task.State
	task.State = proto.TaskStateRunning
	retryable, err := gm.UpdateGlobalTaskAndAddSubTasks(task, nil, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task5, err := gm.GetGlobalTasksInStates(proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task.State, task5[0].State)

	task6, err := gm.GetGlobalTaskByKey("key1")
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task.State, task6.State)

	// test cannot insert task with dup key
	_, err = gm.AddNewGlobalTask("key1", "test2", 4, []byte("test2"))
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'key1' for key 'tidb_global_task.task_key'")

	// test cancel global task
	id, err = gm.AddNewGlobalTask("key2", "test", 4, []byte("test"))
	require.NoError(t, err)

	cancelling, err := gm.IsGlobalTaskCancelling(id)
	require.NoError(t, err)
	require.False(t, cancelling)

	require.NoError(t, gm.CancelGlobalTask(id))
	cancelling, err = gm.IsGlobalTaskCancelling(id)
	require.NoError(t, err)
	require.True(t, cancelling)
}

func TestSubTaskTable(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()

	err := sm.AddNewSubTask(1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false)
	require.NoError(t, err)

	nilTask, err := sm.GetFirstSubtaskInStates("tidb2", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Nil(t, nilTask)

	subtask, err := sm.GetFirstSubtaskInStates("tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, proto.TaskTypeExample, subtask.Type)
	require.Equal(t, int64(1), subtask.TaskID)
	require.Equal(t, proto.TaskStatePending, subtask.State)
	require.Equal(t, "tidb1", subtask.SchedulerID)
	require.Equal(t, []byte("test"), subtask.Meta)
	require.Zero(t, subtask.StartTime)
	require.Zero(t, subtask.UpdateTime)

	subtask2, err := sm.GetFirstSubtaskInStates("tidb1", 1, proto.StepInit, proto.TaskStatePending, proto.TaskStateReverted)
	require.NoError(t, err)
	require.Equal(t, subtask, subtask2)

	ids, err := sm.GetSchedulerIDsByTaskID(1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, "tidb1", ids[0])

	ids, err = sm.GetSchedulerIDsByTaskID(3)
	require.NoError(t, err)
	require.Len(t, ids, 0)

	cnt, err := sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	ok, err := sm.HasSubtasksInStates("tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.True(t, ok)

	ts := time.Now()
	time.Sleep(time.Second)
	require.NoError(t, sm.StartSubtask(1))

	subtask, err = sm.GetFirstSubtaskInStates("tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Nil(t, subtask)

	subtask, err = sm.GetFirstSubtaskInStates("tidb1", 1, proto.StepInit, proto.TaskStateRunning)
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
	require.NoError(t, sm.UpdateSubtaskStateAndError(1, proto.TaskStateCancelling, nil))
	subtask2, err = sm.GetFirstSubtaskInStates("tidb1", 1, proto.StepInit, proto.TaskStateCancelling)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateCancelling, subtask2.State)
	require.Greater(t, subtask2.UpdateTime, subtask.UpdateTime)

	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)

	ok, err = sm.HasSubtasksInStates("tidb1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.False(t, ok)

	err = sm.DeleteSubtasksByTaskID(1)
	require.NoError(t, err)

	ok, err = sm.HasSubtasksInStates("tidb1", 1, proto.StepInit, proto.TaskStatePending, proto.TaskStateRunning)
	require.NoError(t, err)
	require.False(t, ok)

	err = sm.AddNewSubTask(2, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, true)
	require.NoError(t, err)

	cnt, err = sm.GetSubtaskInStatesCnt(2, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	subtasks, err := sm.GetSucceedSubtasksByStep(2, proto.StepInit)
	require.NoError(t, err)
	require.Len(t, subtasks, 0)

	err = sm.FinishSubtask(2, []byte{})
	require.NoError(t, err)

	subtasks, err = sm.GetSucceedSubtasksByStep(2, proto.StepInit)
	require.NoError(t, err)
	require.Len(t, subtasks, 1)

	rowCount, err := sm.GetSubtaskRowCount(2, proto.StepInit)
	require.NoError(t, err)
	require.Equal(t, int64(0), rowCount)
	err = sm.UpdateSubtaskRowCount(2, 100)
	require.NoError(t, err)
	rowCount, err = sm.GetSubtaskRowCount(2, proto.StepInit)
	require.NoError(t, err)
	require.Equal(t, int64(100), rowCount)

	// test UpdateErrorToSubtask do update start/update time
	err = sm.AddNewSubTask(3, proto.StepInit, "for_test", []byte("test"), proto.TaskTypeExample, false)
	require.NoError(t, err)
	require.NoError(t, sm.UpdateErrorToSubtask("for_test", 3, errors.New("fail")))
	subtask, err = sm.GetFirstSubtaskInStates("for_test", 3, proto.StepInit, proto.TaskStateFailed)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateFailed, subtask.State)
	require.Greater(t, subtask.StartTime, ts)
	require.Greater(t, subtask.UpdateTime, ts)

	// test FinishSubtask do update update time
	err = sm.AddNewSubTask(4, proto.StepInit, "for_test1", []byte("test"), proto.TaskTypeExample, false)
	require.NoError(t, err)
	subtask, err = sm.GetFirstSubtaskInStates("for_test1", 4, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.NoError(t, sm.StartSubtask(subtask.ID))
	subtask, err = sm.GetFirstSubtaskInStates("for_test1", 4, proto.StepInit, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Greater(t, subtask.StartTime, ts)
	require.Greater(t, subtask.UpdateTime, ts)
	time.Sleep(time.Second)
	require.NoError(t, sm.FinishSubtask(subtask.ID, []byte{}))
	subtask2, err = sm.GetFirstSubtaskInStates("for_test1", 4, proto.StepInit, proto.TaskStateSucceed)
	require.NoError(t, err)
	require.Equal(t, subtask2.StartTime, subtask.StartTime)
	require.Greater(t, subtask2.UpdateTime, subtask.UpdateTime)

	// test UpdateFailedSchedulerIDs and IsSchedulerCanceled
	canceled, err := sm.IsSchedulerCanceled("for_test999", 4)
	require.NoError(t, err)
	require.True(t, canceled)
	canceled, err = sm.IsSchedulerCanceled("for_test1", 4)
	require.NoError(t, err)
	require.False(t, canceled)
	canceled, err = sm.IsSchedulerCanceled("for_test2", 4)
	require.NoError(t, err)
	require.True(t, canceled)

	require.NoError(t, sm.UpdateSubtaskStateAndError(4, proto.TaskStateRunning, nil))
	require.NoError(t, sm.UpdateFailedSchedulerIDs(4, map[string]string{
		"for_test1": "for_test999",
		"for_test2": "for_test999",
	}))

	canceled, err = sm.IsSchedulerCanceled("for_test1", 4)
	require.NoError(t, err)
	require.True(t, canceled)
	canceled, err = sm.IsSchedulerCanceled("for_test2", 4)
	require.NoError(t, err)
	require.True(t, canceled)
	canceled, err = sm.IsSchedulerCanceled("for_test999", 4)
	require.NoError(t, err)
	require.False(t, canceled)
}

func TestBothGlobalAndSubTaskTable(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()

	id, err := sm.AddNewGlobalTask("key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := sm.GetNewGlobalTask()
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
	retryable, err := sm.UpdateGlobalTaskAndAddSubTasks(task, subTasks, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task, err = sm.GetGlobalTaskByID(1)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateRunning, task.State)

	subtask1, err := sm.GetFirstSubtaskInStates("instance1", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(1), subtask1.ID)
	require.Equal(t, proto.TaskTypeExample, subtask1.Type)
	require.Equal(t, []byte("m1"), subtask1.Meta)

	subtask2, err := sm.GetFirstSubtaskInStates("instance2", 1, proto.StepInit, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(2), subtask2.ID)
	require.Equal(t, proto.TaskTypeExample, subtask2.Type)
	require.Equal(t, []byte("m2"), subtask2.Meta)

	cnt, err := sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending)
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
	retryable, err = sm.UpdateGlobalTaskAndAddSubTasks(task, subTasks, prevState)
	require.NoError(t, err)
	require.Equal(t, true, retryable)

	task, err = sm.GetGlobalTaskByID(1)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateReverting, task.State)

	subtask1, err = sm.GetFirstSubtaskInStates("instance3", 1, proto.StepInit, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(3), subtask1.ID)
	require.Equal(t, proto.TaskTypeExample, subtask1.Type)
	require.Equal(t, []byte("m3"), subtask1.Meta)

	subtask2, err = sm.GetFirstSubtaskInStates("instance4", 1, proto.StepInit, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(4), subtask2.ID)
	require.Equal(t, proto.TaskTypeExample, subtask2.Type)
	require.Equal(t, []byte("m4"), subtask2.Meta)

	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt)

	// test transactional
	require.NoError(t, sm.DeleteSubtasksByTaskID(1))
	failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/MockUpdateTaskErr", "1*return(true)")
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/MockUpdateTaskErr"))
	}()
	prevState = task.State
	task.State = proto.TaskStateFailed
	retryable, err = sm.UpdateGlobalTaskAndAddSubTasks(task, subTasks, prevState)
	require.EqualError(t, err, "updateTaskErr")
	require.Equal(t, true, retryable)

	task, err = sm.GetGlobalTaskByID(1)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateReverting, task.State)

	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)
}

func TestDistFrameworkMeta(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()

	require.NoError(t, sm.StartManager(":4000", "background"))
	require.NoError(t, sm.StartManager(":4001", ""))
	require.NoError(t, sm.StartManager(":4002", "background"))
	nodes, err := sm.GetNodesByRole("background")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		":4000": true,
		":4002": true,
	}, nodes)

	nodes, err = sm.GetNodesByRole("")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		":4001": true,
	}, nodes)
}

func TestSubtaskHistoryTable(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()

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

	require.NoError(t, sm.AddNewSubTask(taskID, proto.StepInit, tidb1, []byte(meta), proto.TaskTypeExample, false))
	require.NoError(t, sm.FinishSubtask(subTask1, []byte(finishedMeta)))
	require.NoError(t, sm.AddNewSubTask(taskID, proto.StepInit, tidb2, []byte(meta), proto.TaskTypeExample, false))
	require.NoError(t, sm.UpdateSubtaskStateAndError(subTask2, proto.TaskStateCanceled, nil))
	require.NoError(t, sm.AddNewSubTask(taskID, proto.StepInit, tidb3, []byte(meta), proto.TaskTypeExample, false))
	require.NoError(t, sm.UpdateSubtaskStateAndError(subTask3, proto.TaskStateFailed, nil))

	subTasks, err := storage.GetSubtasksByTaskIDForTest(sm, taskID)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)
	historySubTasksCnt, err := storage.GetSubtasksFromHistoryForTest(sm)
	require.NoError(t, err)
	require.Equal(t, 0, historySubTasksCnt)
	subTasks, err = sm.GetSubtasksForImportInto(taskID, proto.StepInit)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)

	// test TransferSubTasks2History
	require.NoError(t, sm.TransferSubTasks2History(taskID))

	subTasks, err = storage.GetSubtasksByTaskIDForTest(sm, taskID)
	require.NoError(t, err)
	require.Len(t, subTasks, 0)
	historySubTasksCnt, err = storage.GetSubtasksFromHistoryForTest(sm)
	require.NoError(t, err)
	require.Equal(t, 3, historySubTasksCnt)
	subTasks, err = sm.GetSubtasksForImportInto(taskID, proto.StepInit)
	require.NoError(t, err)
	require.Len(t, subTasks, 3)

	// test GC history table.
	failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds", "return(1)")
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds"))
	}()
	time.Sleep(2 * time.Second)

	require.NoError(t, sm.AddNewSubTask(taskID2, proto.StepInit, tidb1, []byte(meta), proto.TaskTypeExample, false))
	require.NoError(t, sm.UpdateSubtaskStateAndError(subTask4, proto.TaskStateFailed, nil))
	require.NoError(t, sm.TransferSubTasks2History(taskID2))

	require.NoError(t, sm.GCSubtasks())

	historySubTasksCnt, err = storage.GetSubtasksFromHistoryForTest(sm)
	require.NoError(t, err)
	require.Equal(t, 1, historySubTasksCnt)
}

func TestTaskHistoryTable(t *testing.T) {
	pool := GetResourcePool(t)
	gm := GetTaskManager(t, pool)
	defer pool.Close()

	_, err := gm.AddNewGlobalTask("1", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	taskID, err := gm.AddNewGlobalTask("2", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)

	tasks, err := gm.GetGlobalTasksInStates(proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 2, len(tasks))

	require.NoError(t, gm.TransferTasks2History(tasks))

	tasks, err = gm.GetGlobalTasksInStates(proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 0, len(tasks))
	num, err := storage.GetTasksFromHistoryForTest(gm)
	require.NoError(t, err)
	require.Equal(t, 2, num)

	task, err := gm.GetTaskByIDWithHistory(taskID)
	require.NoError(t, err)
	require.NotNil(t, task)

	task, err = gm.GetGlobalTaskByKeyWithHistory("1")
	require.NoError(t, err)
	require.NotNil(t, task)

	// task with fail transfer
	_, err = gm.AddNewGlobalTask("3", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	tasks, err = gm.GetGlobalTasksInStates(proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	tasks[0].Error = errors.New("mock err")
	require.NoError(t, gm.TransferTasks2History(tasks))
	num, err = storage.GetTasksFromHistoryForTest(gm)
	require.NoError(t, err)
	require.Equal(t, 3, num)
}

func TestPauseAndResume(t *testing.T) {
	pool := GetResourcePool(t)
	sm := GetTaskManager(t, pool)
	defer pool.Close()
	require.NoError(t, sm.AddNewSubTask(1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
	require.NoError(t, sm.AddNewSubTask(1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
	require.NoError(t, sm.AddNewSubTask(1, proto.StepInit, "tidb1", []byte("test"), proto.TaskTypeExample, false))
	// 1.1 pause all subtasks.
	require.NoError(t, sm.PauseSubtasks("tidb1", 1))
	cnt, err := sm.GetSubtaskInStatesCnt(1, proto.TaskStatePaused)
	require.NoError(t, err)
	require.Equal(t, int64(3), cnt)
	// 1.2 resume all subtasks.
	require.NoError(t, sm.ResumeSubtasks(1))
	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(3), cnt)

	// 2.1 pause 2 subtasks.
	sm.UpdateSubtaskStateAndError(1, proto.TaskStateSucceed, nil)
	require.NoError(t, sm.PauseSubtasks("tidb1", 1))
	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStatePaused)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt)
	// 2.2 resume 2 subtasks.
	require.NoError(t, sm.ResumeSubtasks(1))
	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt)
}
