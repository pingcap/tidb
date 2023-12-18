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

package taskexecutor_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func runOneTask(ctx context.Context, t *testing.T, mgr *storage.TaskManager, taskKey string, subtaskCnt int) {
	taskID, err := mgr.CreateTask(ctx, taskKey, proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	task, err := mgr.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	// 1. stepOne
	task.Step = proto.StepOne
	task.State = proto.TaskStateRunning
	_, err = mgr.UpdateTaskAndAddSubTasks(ctx, task, nil, proto.TaskStatePending)
	require.NoError(t, err)
	for i := 0; i < subtaskCnt; i++ {
		testutil.CreateSubTask(t, mgr, taskID, proto.StepOne, "test", nil, proto.TaskTypeExample, 11, false)
	}
	task, err = mgr.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	factory := taskexecutor.GetTaskExecutorFactory(task.Type)
	require.NotNil(t, factory)
	executor := factory(ctx, "test", task, mgr)
	require.NoError(t, executor.Run(ctx, task))
	// 2. stepTwo
	task.Step = proto.StepTwo
	_, err = mgr.UpdateTaskAndAddSubTasks(ctx, task, nil, proto.TaskStateRunning)
	require.NoError(t, err)
	for i := 0; i < subtaskCnt; i++ {
		testutil.CreateSubTask(t, mgr, taskID, proto.StepTwo, "test", nil, proto.TaskTypeExample, 11, false)
	}
	task, err = mgr.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.NoError(t, executor.Run(ctx, task))
}

func TestTaskExecutorBasic(t *testing.T) {
	// must disable disttask framework to ensure the test pure.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/breakInTaskExecutorUT", "return()"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/breakInTaskExecutorUT"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mgr := storage.NewTaskManager(pool)

	testutil.InitTaskExecutor(ctrl, func(ctx context.Context, subtask *proto.Subtask) error {
		switch subtask.Step {
		case proto.StepOne:
			logutil.BgLogger().Info("run step one")
		case proto.StepTwo:
			logutil.BgLogger().Info("run step two")
		default:
			panic("invalid step")
		}
		return nil
	})
	for i := 0; i < 10; i++ {
		runOneTask(ctx, t, mgr, "key"+strconv.Itoa(i), i)
	}
}
