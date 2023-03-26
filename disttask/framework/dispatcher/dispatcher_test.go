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
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func MockDispatcher(t *testing.T) (dispatcher.Dispatch, *storage.GlobalTaskManager, *storage.SubTaskManager) {
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
	return dsp, gm, sm
}

func TestGetInstance(t *testing.T) {
	ctx := context.Background()
	dsp, _, subTaskMgr := MockDispatcher(t)

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
	instanceIDs, err := dsp.GetTaskAllInstances(ctx, 1)
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
	instanceIDs, err = dsp.GetTaskAllInstances(ctx, 1)
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
	instanceIDs, err = dsp.GetTaskAllInstances(ctx, gTaskID)
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
	instanceIDs, err = dsp.GetTaskAllInstances(ctx, gTaskID)
	require.NoError(t, err)
	require.Len(t, instanceIDs, len(uuids))
	require.ElementsMatch(t, instanceIDs, uuids)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/mockGetAllServerInfo"))
}

const (
	subtaskCnt = 10
)

func equalGTask(t *testing.T, expect, actual *proto.Task) {
	require.Equal(t, expect.ID, actual.ID)
	require.Equal(t, expect.Type, actual.Type)
	require.Equal(t, expect.State, actual.State)
	require.Equal(t, expect.Meta, actual.Meta)
	require.Equal(t, expect.Step, actual.Step)
	require.Equal(t, expect.DispatcherID, actual.DispatcherID)
	require.Equal(t, expect.Concurrency, actual.Concurrency)
	// Delete the digit of fractional seconds part.
	expect.StateUpdateTime = expect.StateUpdateTime.Round(1 * time.Second)
	eTime := expect.StateUpdateTime.Format("2006-01-02 15:04:05")
	aTime := actual.StateUpdateTime.Format("2006-01-02 15:04:05")
	require.Equal(t, eTime, aTime)
}

func TestSimple(t *testing.T) {
	dsp, gTaskMgr, subTaskMgr := MockDispatcher(t)
	dsp.Start()
	defer dsp.Stop()

	dispatcher.RegisterTaskFlowHandle(taskTypeExample, NumberExampleHandle{})
	taskID, err := gTaskMgr.AddNewTask(taskTypeExample, 0, nil)
	require.NoError(t, err)

	// test DispatchTaskLoop
	cnt := 40
	taskMap := make(map[int64]*proto.Task)
	for i := 0; i < cnt; i++ {
		taskMap = dsp.(dispatcher.DispatcherForTest).GetRunningGlobalTasks()
		if len(taskMap) == 1 {
			break
		}
		time.Sleep(time.Millisecond * 50)
	}
	require.Len(t, taskMap, 1)
	tasks, err := gTaskMgr.GetTasksInStates(proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, tasks[0].ID, taskID)
	require.Equal(t, tasks[0].Concurrency, uint64(dispatcher.DefaultSubtaskConcurrency))
	equalGTask(t, taskMap[taskID], tasks[0])
	subtasks, err := subTaskMgr.GetSubtaskInStatesCnt(taskID, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, subtasks, int64(subtaskCnt))
	// test DetectTaskLoop
	for i := 1; i <= subtaskCnt; i++ {
		err = subTaskMgr.UpdateSubtaskState(int64(i), proto.TaskStateSucceed)
		require.NoError(t, err)
	}
	for i := 0; i < cnt; i++ {
		tasks, err = gTaskMgr.GetTasksInStates(proto.TaskStateSucceed)
		require.NoError(t, err)
		if len(tasks) == 1 {
			break
		}
		time.Sleep(time.Millisecond * 50)
	}
	require.Len(t, tasks, 1)
	require.Equal(t, tasks[0].State, proto.TaskStateSucceed)
	taskMap = dsp.(dispatcher.DispatcherForTest).GetRunningGlobalTasks()
	require.Len(t, taskMap, 0)
}

const taskTypeExample = "task_example"

type NumberExampleHandle struct {
}

func (n NumberExampleHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.Dispatch, gTask *proto.Task) (metas [][]byte, err error) {
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

func (n NumberExampleHandle) ProcessErrFlow(_ context.Context, _ dispatcher.Dispatch, _ *proto.Task, _ string) (meta []byte, err error) {
	// Don't handle not.
	return nil, nil
}
