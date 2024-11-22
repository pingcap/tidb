// Copyright 2024 PingCAP, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestModifyTaskConcurrency(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 1, 16, true)
	schedulerExt := testutil.GetMockSchedulerExt(c.MockCtrl, testutil.SchedulerInfo{
		AllErrorRetryable: true,
		StepInfos: []testutil.StepInfo{
			{Step: proto.StepOne, SubtaskCnt: 1},
			{Step: proto.StepTwo, SubtaskCnt: 1},
		},
	})
	subtaskCh := make(chan struct{})
	testutil.RegisterTaskMeta(t, c.MockCtrl, schedulerExt, c.TestContext,
		func(ctx context.Context, subtask *proto.Subtask) error {
			<-subtaskCh
			return nil
		},
	)

	t.Run("modify task concurrency at step two", func(t *testing.T) {
		var once sync.Once
		modifiySyncCh := make(chan struct{})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/beforeRefreshTask", func(task *proto.Task) {
			if task.State != proto.TaskStateRunning && task.Step != proto.StepTwo {
				return
			}
			once.Do(func() {
				require.NoError(t, c.TaskMgr.ModifyTaskByID(c.Ctx, task.ID, &proto.ModifyParam{
					PrevState: proto.TaskStateRunning,
					Modifications: []proto.Modification{
						{Type: proto.ModifyConcurrency, To: 7},
					},
				}))
				<-modifiySyncCh
			})
		})
		task, err := handle.SubmitTask(c.Ctx, "k1", proto.TaskTypeExample, 3, "", nil)
		require.NoError(t, err)
		// finish StepOne
		subtaskCh <- struct{}{}
		// wait task move to 'modifying' state
		modifiySyncCh <- struct{}{}
		// wait task move back to 'running' state
		require.Eventually(t, func() bool {
			gotTask, err2 := c.TaskMgr.GetTaskByID(c.Ctx, task.ID)
			require.NoError(t, err2)
			return gotTask.State == proto.TaskStateRunning
		}, 10*time.Second, 100*time.Millisecond)
		// finish StepTwo
		subtaskCh <- struct{}{}
		checkSubtaskConcurrency(t, c, task.ID, map[proto.Step]int{
			proto.StepOne: 3,
			proto.StepTwo: 7,
		})
	})
}

func checkSubtaskConcurrency(t *testing.T, c *testutil.TestDXFContext, taskID int64, expectedStepCon map[proto.Step]int) {
	for step, con := range expectedStepCon {
		subtasks, err := c.TaskMgr.GetSubtasksWithHistory(c.Ctx, taskID, step)
		require.NoError(t, err)
		require.Len(t, subtasks, 1)
		require.Equal(t, con, subtasks[0].Concurrency)
	}
}
