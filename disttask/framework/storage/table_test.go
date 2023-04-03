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

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestGlobalTaskTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	gm := storage.NewGlobalTaskManager(context.Background(), tk.Session())

	storage.SetGlobalTaskManager(gm)
	gm, err := storage.GetGlobalTaskManager()
	require.NoError(t, err)

	id, err := gm.AddNewTask("key1", "test", 4, []byte("test"))
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	task, err := gm.GetNewTask()
	require.NoError(t, err)
	require.Equal(t, int64(1), task.ID)
	require.Equal(t, "key1", task.Key)
	require.Equal(t, "test", task.Type)
	require.Equal(t, proto.TaskStatePending, task.State)
	require.Equal(t, uint64(4), task.Concurrency)
	require.Equal(t, []byte("test"), task.Meta)

	task2, err := gm.GetTaskByID(1)
	require.NoError(t, err)
	require.Equal(t, task, task2)

	task3, err := gm.GetTasksInStates(proto.TaskStatePending)
	require.NoError(t, err)
	require.Len(t, task3, 1)
	require.Equal(t, task, task3[0])

	task4, err := gm.GetTasksInStates(proto.TaskStatePending, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task4, 1)
	require.Equal(t, task, task4[0])

	task.State = proto.TaskStateRunning
	err = gm.UpdateTask(task)
	require.NoError(t, err)

	task5, err := gm.GetTasksInStates(proto.TaskStateRunning)
	require.NoError(t, err)
	require.Len(t, task5, 1)
	require.Equal(t, task, task5[0])

	task6, err := gm.GetTaskByKey("key1")
	require.NoError(t, err)
	require.Equal(t, task, task6)

	// test cannot insert task with dup key
	_, err = gm.AddNewTask("key1", "test2", 4, []byte("test2"))
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'key1' for key 'tidb_global_task.task_key'")
}

func TestSubTaskTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	sm := storage.NewSubTaskManager(context.Background(), tk.Session())

	storage.SetSubTaskManager(sm)
	sm, err := storage.GetSubTaskManager()
	require.NoError(t, err)

	err = sm.AddNewTask(1, "tidb1", []byte("test"), proto.TaskTypeExample, false)
	require.NoError(t, err)

	nilTask, err := sm.GetSubtaskInStates("tidb2", 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Nil(t, nilTask)

	task, err := sm.GetSubtaskInStates("tidb1", 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, proto.TaskTypeExample, task.Type)
	require.Equal(t, int64(1), task.TaskID)
	require.Equal(t, proto.TaskStatePending, task.State)
	require.Equal(t, "tidb1", task.SchedulerID)
	require.Equal(t, []byte("test"), task.Meta)

	task2, err := sm.GetSubtaskInStates("tidb1", 1, proto.TaskStatePending, proto.TaskStateReverted)
	require.NoError(t, err)
	require.Equal(t, task, task2)

	ids, err := sm.GetSchedulerIDs(1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, "tidb1", ids[0])

	ids, err = sm.GetSchedulerIDs(3)
	require.NoError(t, err)
	require.Len(t, ids, 0)

	cnt, err := sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	ok, err := sm.HasSubtasksInStates("tidb1", 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.True(t, ok)

	err = sm.UpdateHeartbeat("tidb1", 1, time.Now())
	require.NoError(t, err)

	err = sm.UpdateSubtaskState(1, proto.TaskStateRunning)
	require.NoError(t, err)

	task, err = sm.GetSubtaskInStates("tidb1", 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Nil(t, task)

	task, err = sm.GetSubtaskInStates("tidb1", 1, proto.TaskStateRunning)
	require.NoError(t, err)
	require.Equal(t, proto.TaskTypeExample, task.Type)
	require.Equal(t, int64(1), task.TaskID)
	require.Equal(t, proto.TaskStateRunning, task.State)
	require.Equal(t, "tidb1", task.SchedulerID)
	require.Equal(t, []byte("test"), task.Meta)

	cnt, err = sm.GetSubtaskInStatesCnt(1, proto.TaskStatePending)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)

	ok, err = sm.HasSubtasksInStates("tidb1", 1, proto.TaskStatePending)
	require.NoError(t, err)
	require.False(t, ok)

	err = sm.DeleteTasks(1)
	require.NoError(t, err)

	ok, err = sm.HasSubtasksInStates("tidb1", 1, proto.TaskStatePending, proto.TaskStateRunning)
	require.NoError(t, err)
	require.False(t, ok)

	err = sm.AddNewTask(2, "tidb1", []byte("test"), proto.TaskTypeExample, true)
	require.NoError(t, err)

	cnt, err = sm.GetSubtaskInStatesCnt(2, proto.TaskStateRevertPending)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)
}
