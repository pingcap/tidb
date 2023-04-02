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

package dispatcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func MockDispatcher(t *testing.T) (dispatcher.Dispatch, *storage.GlobalTaskManager, *storage.SubTaskManager, kv.Storage) {
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	stk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	gm := storage.NewGlobalTaskManager(util.WithInternalSourceType(ctx, "globalTaskManager"), gtk.Session())
	storage.SetGlobalTaskManager(gm)
	sm := storage.NewSubTaskManager(util.WithInternalSourceType(ctx, "subTaskManager"), stk.Session())
	storage.SetSubTaskManager(sm)
	dsp, err := dispatcher.NewDispatcher(util.WithInternalSourceType(ctx, "dispatcher"), gm, sm)
	require.NoError(t, err)
	return dsp, gm, sm, store
}

func deleteTasks(t *testing.T, store kv.Storage, taskID int64) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_global_task where id = %d", taskID))
}

func TestGetInstance(t *testing.T) {
	ctx := context.Background()
	dsp, _, subTaskMgr, _ := MockDispatcher(t)

	makeFailpointRes := func(v interface{}) string {
		bytes, err := json.Marshal(v)
		require.NoError(t, err)
		return fmt.Sprintf("return(`%s`)", string(bytes))
	}

	// test no server
	mockedAllServerInfos := map[string]*infosync.ServerInfo{}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockGetAllServerInfo", makeFailpointRes(mockedAllServerInfos)))
	instanceID, err := dispatcher.GetEligibleInstance(ctx)
	require.Lenf(t, instanceID, 0, "instanceID:%d", instanceID)
	require.EqualError(t, err, "not found instance")
	instanceIDs, err := dsp.GetAllSchedulerIDs(ctx, 1)
	require.Lenf(t, instanceIDs, 0, "instanceID:%d", instanceID)
	require.NoError(t, err)

	// test 2 servers
	// server ids: uuid0, uuid1
	// subtask instance ids: nil
	uuids := []string{"ddl_id_1", "ddl_id_2"}
	mockedAllServerInfos = map[string]*infosync.ServerInfo{
		uuids[0]: {
			ID: uuids[0],
		},
		uuids[1]: {
			ID: uuids[1],
		},
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockGetAllServerInfo", makeFailpointRes(mockedAllServerInfos)))
	instanceID, err = dispatcher.GetEligibleInstance(ctx)
	require.NoError(t, err)
	if instanceID != uuids[0] && instanceID != uuids[1] {
		require.FailNowf(t, "expected uuids:%d,%d, actual uuid:%d", uuids[0], uuids[1], instanceID)
	}
	instanceIDs, err = dsp.GetAllSchedulerIDs(ctx, 1)
	require.Lenf(t, instanceIDs, 0, "instanceID:%d", instanceID)
	require.NoError(t, err)

	// server ids: uuid0, uuid1
	// subtask instance ids: uuid1
	gTaskID := int64(1)
	subtask := &proto.Subtask{
		Type:        proto.TaskTypeExample,
		TaskID:      gTaskID,
		SchedulerID: uuids[1],
	}
	err = subTaskMgr.AddNewTask(gTaskID, subtask.SchedulerID, nil, subtask.Type, true)
	require.NoError(t, err)
	instanceIDs, err = dsp.GetAllSchedulerIDs(ctx, gTaskID)
	require.NoError(t, err)
	require.Equal(t, []string{uuids[1]}, instanceIDs)
	// server ids: uuid0, uuid1
	// subtask instance ids: uuid0, uuid1
	subtask = &proto.Subtask{
		Type:        proto.TaskTypeExample,
		TaskID:      gTaskID,
		SchedulerID: uuids[0],
	}
	err = subTaskMgr.AddNewTask(gTaskID, subtask.SchedulerID, nil, subtask.Type, true)
	require.NoError(t, err)
	instanceIDs, err = dsp.GetAllSchedulerIDs(ctx, gTaskID)
	require.NoError(t, err)
	require.Len(t, instanceIDs, len(uuids))
	require.ElementsMatch(t, instanceIDs, uuids)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/mockGetAllServerInfo"))
}

const (
	subtaskCnt = 10
)

func checkDispatch(t *testing.T, taskCnt int, isSucc bool) {
	// test DispatchTaskLoop
	// test parallelism control
	var originalConcurrency int
	if taskCnt == 1 {
		originalConcurrency = dispatcher.DefaultDispatchConcurrency
		dispatcher.DefaultDispatchConcurrency = 1
	}

	dsp, gTaskMgr, subTaskMgr, store := MockDispatcher(t)
	dsp.Start()
	defer func() {
		dsp.Stop()
		// make data race happy
		if taskCnt == 1 {
			dispatcher.DefaultDispatchConcurrency = originalConcurrency
		}
	}()

	dispatcher.RegisterTaskFlowHandle(taskTypeExample, NumberExampleHandle{})

	cnt := 20
	checkGetRunningGTaskCnt := func() {
		var retCnt int
		for i := 0; i < cnt; i++ {
			retCnt = dsp.(dispatcher.DispatcherForTest).GetRunningGTaskCnt()
			if retCnt == taskCnt {
				break
			}
			time.Sleep(time.Millisecond * 30)
		}
		require.Equal(t, retCnt, taskCnt)
	}

	// Mock add tasks.
	taskIDs := make([]int64, 0, taskCnt)
	for i := 0; i < taskCnt; i++ {
		taskID, err := gTaskMgr.AddNewTask(fmt.Sprintf("%d", i), taskTypeExample, 0, nil)
		require.NoError(t, err)
		taskIDs = append(taskIDs, taskID)
	}
	// test normal flow
	checkGetRunningGTaskCnt()
	tasks, err := gTaskMgr.GetTasksInStates(proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, tasks, taskCnt)
	for i, taskID := range taskIDs {
		require.Equal(t, int64(i+1), tasks[i].ID)
		subtasks, err := subTaskMgr.GetSubtaskInStatesCnt(taskID, proto.TaskStatePending)
		require.NoError(t, err)
		require.Equal(t, subtasks, int64(subtaskCnt))
	}
	// test parallelism control
	taskID, err := gTaskMgr.AddNewTask(fmt.Sprintf("%d", taskCnt), taskTypeExample, 0, nil)
	require.NoError(t, err)
	checkGetRunningGTaskCnt()
	// Clean the task.
	deleteTasks(t, store, taskID)

	// test DetectTaskLoop
	checkGetGTaskState := func(expectedState string) {
		for i := 0; i < cnt; i++ {
			tasks, err = gTaskMgr.GetTasksInStates(expectedState)
			require.NoError(t, err)
			if len(tasks) == taskCnt {
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
	}
	// Test all subtasks are successful.
	if isSucc {
		// Mock subtasks succeed.
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = subTaskMgr.UpdateSubtaskState(int64(i), proto.TaskStateSucceed)
			require.NoError(t, err)
		}
		checkGetGTaskState(proto.TaskStateSucceed)
		require.Len(t, tasks, taskCnt)
		require.Equal(t, 0, dsp.(dispatcher.DispatcherForTest).GetRunningGTaskCnt())
		return
	}

	// Test each task has a subtask failed.
	failpoint.Enable("github.com/pingcap/tidb/disttask/framework/storage/MockUpdateTaskErr", "1*return(true)")
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/storage/MockUpdateTaskErr"))
	}()
	// Mock a subtask fails.
	for i := 1; i <= subtaskCnt*taskCnt; i += subtaskCnt {
		err = subTaskMgr.UpdateSubtaskState(int64(i), proto.TaskStateFailed)
		require.NoError(t, err)
	}
	checkGetGTaskState(proto.TaskStateReverting)
	require.Len(t, tasks, taskCnt)
	// Mock all subtask reverted.
	start := subtaskCnt * taskCnt
	for i := start; i <= start+subtaskCnt*taskCnt; i++ {
		err = subTaskMgr.UpdateSubtaskState(int64(i), proto.TaskStateReverted)
		require.NoError(t, err)
	}
	checkGetGTaskState(proto.TaskStateReverted)
	require.Len(t, tasks, taskCnt)
	require.Equal(t, 0, dsp.(dispatcher.DispatcherForTest).GetRunningGTaskCnt())
}

func TestSimpleNormalFlow(t *testing.T) {
	checkDispatch(t, 1, true)
}

func TestSimpleErrFlow(t *testing.T) {
	checkDispatch(t, 1, false)
}

func TestParallelNormalFlow(t *testing.T) {
	checkDispatch(t, 3, true)
}

func TestParallelErrFlow(t *testing.T) {
	checkDispatch(t, 3, false)
}

const taskTypeExample = "task_example"

type NumberExampleHandle struct {
}

func (n NumberExampleHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State == proto.TaskStatePending {
		gTask.Step = proto.StepInit
	}
	switch gTask.Step {
	case proto.StepInit:
		gTask.Step = proto.StepOne
		for i := 0; i < subtaskCnt; i++ {
			metas = append(metas, []byte{'1'})
		}
		logutil.BgLogger().Info("progress step init")
	case proto.StepOne:
		logutil.BgLogger().Info("progress step one")
		return nil, nil
	default:
		return nil, errors.New("unknown step")
	}
	return metas, nil
}

func (n NumberExampleHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ string) (meta []byte, err error) {
	// Don't handle not.
	return nil, nil
}
