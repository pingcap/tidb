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

package integrationtests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestOnTaskError(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 2, 16, true)
	scope := handle.GetTargetScope()

	t.Run("retryable error on OnNextSubtasksBatch", func(t *testing.T) {
		registerExampleTask(t, c.MockCtrl, testutil.GetPlanErrSchedulerExt(c.MockCtrl, c.TestContext), c.TestContext, nil)
		submitTaskAndCheckSuccessForBasic(c.Ctx, t, "key1", c.TestContext)
	})

	t.Run("non retryable error on OnNextSubtasksBatch", func(t *testing.T) {
		registerExampleTask(t, c.MockCtrl, testutil.GetPlanNotRetryableErrSchedulerExt(c.MockCtrl), c.TestContext, nil)
		task := testutil.SubmitAndWaitTask(c.Ctx, t, "key2-1", scope, 1)
		require.Equal(t, proto.TaskStateReverted, task.State)
		registerExampleTask(t, c.MockCtrl, testutil.GetStepTwoPlanNotRetryableErrSchedulerExt(c.MockCtrl), c.TestContext, nil)
		task = testutil.SubmitAndWaitTask(c.Ctx, t, "key2-2", scope, 1)
		require.Equal(t, proto.TaskStateReverted, task.State)
	})

	prepareForAwaitingResolutionTestFn := func(t *testing.T, taskKey string) int64 {
		subtaskErrRetryable := atomic.Bool{}
		executorExt := testutil.GetTaskExecutorExt(c.MockCtrl,
			func(task *proto.Task) (execute.StepExecutor, error) {
				return testutil.GetCommonStepExecutor(c.MockCtrl, task.Step, func(ctx context.Context, subtask *proto.Subtask) error {
					if !subtaskErrRetryable.Load() {
						return errors.New("non retryable subtask error")
					}
					return nil
				}), nil
			},
			func(error) bool {
				return subtaskErrRetryable.Load()
			},
		)
		testutil.RegisterExampleTask(t, testutil.GetPlanErrSchedulerExt(c.MockCtrl, c.TestContext),
			executorExt, testutil.GetCommonCleanUpRoutine(c.MockCtrl))
		tm, err := storage.GetTaskManager()
		require.NoError(t, err)
		taskID, err := tm.CreateTask(c.Ctx, taskKey, proto.TaskTypeExample, "", 1, scope, 2, proto.ExtraParams{ManualRecovery: true}, nil)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			task, err := tm.GetTaskByID(c.Ctx, taskID)
			require.NoError(t, err)
			return task.State == proto.TaskStateAwaitingResolution
		}, 10*time.Second, 100*time.Millisecond)
		subtaskErrRetryable.Store(true)
		return taskID
	}

	t.Run("task enter awaiting-resolution state if ManualRecovery set, success after manual recover", func(t *testing.T) {
		taskKey := "key3-1"
		taskID := prepareForAwaitingResolutionTestFn(t, taskKey)
		tk := testkit.NewTestKit(t, c.Store)
		tk.MustExec(fmt.Sprintf("update mysql.tidb_background_subtask set state='pending' where state='failed' and task_key= %d", taskID))
		tk.MustExec(fmt.Sprintf("update mysql.tidb_global_task set state='running' where id = %d", taskID))
		task := testutil.WaitTaskDone(c.Ctx, t, taskKey)
		require.Equal(t, proto.TaskStateSucceed, task.State)
	})

	t.Run("task enter awaiting-resolution state if ManualRecovery set, cancel also works", func(t *testing.T) {
		taskKey := "key4-1"
		taskID := prepareForAwaitingResolutionTestFn(t, taskKey)
		require.NoError(t, c.TaskMgr.CancelTask(c.Ctx, taskID))
		task := testutil.WaitTaskDone(c.Ctx, t, taskKey)
		require.Equal(t, proto.TaskStateReverted, task.State)
	})
}
