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
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestTaskState(t *testing.T) {
	_, gm, ctx := testutil.InitTableTest(t)

	require.NoError(t, gm.InitMeta(ctx, ":4000", ""))

	// 1. cancel task
	id, err := gm.CreateTask(ctx, "key1", "test", 4, "", []byte("test"))
	require.NoError(t, err)
	// require.Equal(t, int64(1), id) TODO: unstable for infoschema v2
	require.NoError(t, gm.CancelTask(ctx, id))
	task, err := gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateCancelling, proto.StepInit)

	// 2. cancel task by key session
	id, err = gm.CreateTask(ctx, "key2", "test", 4, "", []byte("test"))
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
	id, err = gm.CreateTask(ctx, "key3", "test", 4, "", []byte("test"))
	require.NoError(t, err)
	// require.Equal(t, int64(3), id) TODO: unstable for infoschema v2
	failedErr := errors.New("test err")
	require.NoError(t, gm.FailTask(ctx, id, proto.TaskStatePending, failedErr))
	task, err = gm.GetTaskByID(ctx, id)
	require.NoError(t, err)
	checkTaskStateStep(t, task, proto.TaskStateFailed, proto.StepInit)
	require.ErrorContains(t, task.Error, "test err")

	// 4. Reverted task
	id, err = gm.CreateTask(ctx, "key4", "test", 4, "", []byte("test"))
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
	id, err = gm.CreateTask(ctx, "key5", "test", 4, "", []byte("test"))
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
	id, err = gm.CreateTask(ctx, "key6", "test", 4, "", []byte("test"))
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
