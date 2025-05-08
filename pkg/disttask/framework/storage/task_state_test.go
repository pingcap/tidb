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
	"cmp"
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestTaskState(t *testing.T) {
	_, gm, ctx := testutil.InitTableTest(t)

	require.NoError(t, gm.InitMeta(ctx, ":4000", ""))

	// 1. cancel task
	id, err := gm.CreateTask(ctx, "key1", "test", 4, "", 0, proto.ExtraParams{}, []byte("test"))
	require.NoError(t, err)
	// require.Equal(t, int64(1), id) TODO: unstable for infoschema v2
	require.NoError(t, gm.CancelTask(ctx, id))
	task, err := gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateCancelling, proto.StepInit)

	// 2. cancel task by key session
	id, err = gm.CreateTask(ctx, "key2", "test", 4, "", 0, proto.ExtraParams{}, []byte("test"))
	require.NoError(t, err)
	// require.Equal(t, int64(2), id) TODO: unstable for infoschema v2
	require.NoError(t, gm.WithNewTxn(ctx, func(se sessionctx.Context) error {
		ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
		return gm.CancelTaskByKeySession(ctx, se, "key2")
	}))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateCancelling, proto.StepInit)

	// 3. fail task
	id, err = gm.CreateTask(ctx, "key3", "test", 4, "", 0, proto.ExtraParams{}, []byte("test"))
	require.NoError(t, err)
	// require.Equal(t, int64(3), id) TODO: unstable for infoschema v2
	failedErr := errors.New("test err")
	require.NoError(t, gm.FailTask(ctx, id, proto.TaskStatePending, failedErr))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateFailed, proto.StepInit)
	require.ErrorContains(t, task.Error, "test err")

	// 4. Reverted task
	id, err = gm.CreateTask(ctx, "key4", "test", 4, "", 0, proto.ExtraParams{}, []byte("test"))
	require.NoError(t, err)
	// require.Equal(t, int64(4), id) TODO: unstable for infoschema v2
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStatePending, proto.StepInit)
	err = gm.RevertTask(ctx, task.ID, proto.TaskStatePending, nil)
	require.NoError(t, err)
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateReverting, proto.StepInit)

	require.NoError(t, gm.RevertedTask(ctx, id))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateReverted, proto.StepInit)

	// 5. pause task
	id, err = gm.CreateTask(ctx, "key5", "test", 4, "", 0, proto.ExtraParams{}, []byte("test"))
	require.NoError(t, err)
	// require.Equal(t, int64(5), id) TODO: unstable for infoschema v2
	found, err := gm.PauseTask(ctx, "key5")
	require.NoError(t, err)
	require.True(t, found)
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePausing, task.State)

	// 6. paused task
	require.NoError(t, gm.PausedTask(ctx, id))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePaused, task.State)

	// 7. resume task
	found, err = gm.ResumeTask(ctx, "key5")
	require.NoError(t, err)
	require.True(t, found)
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateResuming, task.State)
	require.NoError(t, gm.ResumedTask(ctx, id))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateRunning, task.State)

	// 8. succeed task
	id, err = gm.CreateTask(ctx, "key6", "test", 4, "", 0, proto.ExtraParams{}, []byte("test"))
	require.NoError(t, err)
	// require.Equal(t, int64(6), id) TODO: unstable for infoschema v2
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStatePending, proto.StepInit)
	require.NoError(t, gm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, nil))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateRunning, proto.StepOne)
	require.NoError(t, gm.SucceedTask(ctx, id))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateSucceed, proto.StepDone)
}

func TestModifyTask(t *testing.T) {
	_, gm, ctx := testutil.InitTableTest(t)
	require.NoError(t, gm.InitMeta(ctx, ":4000", ""))

	id, err := gm.CreateTask(ctx, "key1", "test", 4, "", 0, proto.ExtraParams{}, []byte("test"))
	require.NoError(t, err)

	require.ErrorIs(t, gm.ModifyTaskByID(ctx, id, &proto.ModifyParam{
		PrevState: proto.TaskStateReverting,
	}), storage.ErrTaskStateNotAllow)
	require.ErrorIs(t, gm.ModifyTaskByID(ctx, 123123123, &proto.ModifyParam{
		PrevState: proto.TaskStatePaused,
	}), storage.ErrTaskNotFound)
	require.ErrorIs(t, gm.ModifyTaskByID(ctx, id, &proto.ModifyParam{
		PrevState: proto.TaskStatePaused,
	}), storage.ErrTaskChanged)

	// task changed in middle of modifying
	ch := make(chan struct{})
	var wg tidbutil.WaitGroupWrapper
	var counter atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/storage/beforeMoveToModifying", func() {
		if counter.Add(1) == 1 {
			<-ch
			<-ch
		}
	})
	task, err := gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStatePending, task.State)
	subtasks := make([]*proto.Subtask, 0, 4)
	for i := range 4 {
		subtasks = append(subtasks, proto.NewSubtask(proto.StepOne, task.ID, task.Type,
			":4000", task.Concurrency, proto.EmptyMeta, i+1))
	}
	wg.Run(func() {
		ch <- struct{}{}
		require.NoError(t, gm.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, subtasks))
		ch <- struct{}{}
	})
	require.ErrorIs(t, gm.ModifyTaskByID(ctx, id, &proto.ModifyParam{
		PrevState: proto.TaskStatePending,
	}), storage.ErrTaskChanged)
	wg.Wait()

	// move to 'modifying' success
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateRunning, task.State)
	param := proto.ModifyParam{
		PrevState: proto.TaskStateRunning,
		Modifications: []proto.Modification{
			{Type: proto.ModifyConcurrency, To: 2},
		},
	}
	require.NoError(t, gm.ModifyTaskByID(ctx, id, &param))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateModifying, task.State)
	require.Equal(t, param, task.ModifyParam)

	// modified
	gotSubtasks, err := gm.GetSubtasksWithHistory(ctx, task.ID, proto.StepOne)
	require.NoError(t, err)
	slices.SortFunc(gotSubtasks, func(i, j *proto.Subtask) int {
		return cmp.Compare(i.Ordinal, j.Ordinal)
	})
	require.Len(t, gotSubtasks, len(subtasks))
	require.NoError(t, gm.FinishSubtask(ctx, gotSubtasks[0].ExecID, gotSubtasks[0].ID, nil))
	require.NoError(t, gm.StartSubtask(ctx, gotSubtasks[1].ID, gotSubtasks[1].ExecID))
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `update mysql.tidb_background_subtask set state='paused' where id=%?`,
			gotSubtasks[2].ID)
		return err
	}))
	task.Concurrency = 2
	task.Meta = []byte("modified")
	require.NoError(t, gm.ModifiedTask(ctx, task))
	checkTaskAfterModify(ctx, t, gm, task.ID,
		2, []byte("modified"), []int{4, 2, 2, 2},
	)

	// task state changed before move to 'modified'
	param = proto.ModifyParam{
		PrevState: proto.TaskStateRunning,
		Modifications: []proto.Modification{
			{Type: proto.ModifyConcurrency, To: 3},
		},
	}
	require.NoError(t, gm.ModifyTaskByID(ctx, id, &param))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateModifying, task.State)
	require.Equal(t, param, task.ModifyParam)
	ch = make(chan struct{})
	wg = tidbutil.WaitGroupWrapper{}
	var called bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/storage/beforeModifiedTask", func() {
		if called {
			return
		}
		called = true
		<-ch
		<-ch
	})
	taskClone := *task
	wg.Run(func() {
		ch <- struct{}{}
		// NOTE: this will NOT happen in real case, because the task can NOT move
		// to 'modifying' again to change modify params.
		// here just to show that if another client finishes modifying, our modify
		// will skip silently.
		taskClone.Concurrency = 5
		taskClone.Meta = []byte("modified-other")
		require.NoError(t, gm.ModifiedTask(ctx, &taskClone))
		ch <- struct{}{}
	})
	task.Concurrency = 3
	task.Meta = []byte("modified2")
	require.NoError(t, gm.ModifiedTask(ctx, task))
	wg.Wait()
	checkTaskAfterModify(ctx, t, gm, task.ID,
		5, []byte("modified-other"), []int{4, 5, 5, 5},
	)
}

func checkTaskAfterModify(
	ctx context.Context, t *testing.T, gm *storage.TaskManager, taskID int64,
	expectConcurrency int, expectedMeta []byte, expectedSTConcurrencies []int) {
	task, err := gm.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, proto.TaskStateRunning, task.State)
	require.Equal(t, expectConcurrency, task.Concurrency)
	require.Equal(t, expectedMeta, task.Meta)
	require.Equal(t, proto.ModifyParam{}, task.ModifyParam)
	gotSubtasks, err := gm.GetSubtasksWithHistory(ctx, task.ID, proto.StepOne)
	require.NoError(t, err)
	require.Len(t, gotSubtasks, len(expectedSTConcurrencies))
	slices.SortFunc(gotSubtasks, func(i, j *proto.Subtask) int {
		return cmp.Compare(i.Ordinal, j.Ordinal)
	})
	for i, expected := range expectedSTConcurrencies {
		require.Equal(t, expected, gotSubtasks[i].Concurrency)
	}
}
